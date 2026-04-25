# Refactor Plan

掃過 `cmd/*/main.go` 和 `shortener/store/*` 之後整理的待辦。按嚴重度排序。

---

## 1. Bloom filter 邏輯抽共用（必做）

**問題**

Bloom filter 的 hash → bit position 演算法在兩個地方各寫了一份：

- `shortener/store/redis.go::bloomPositions` + `BloomAdd` + `BloomCheck`
- `cmd/seed/main.go::bloomAdd`

兩邊都硬寫 `bloomKey = "bloom:urls"`、`bloomSize = 1_000_000`、`bloomHashes = 5`，還有一模一樣的 SHA-256 → bit position 邏輯。

**風險**

如果改 seed 那份（例如換 hash 函數、擴大 bloomSize），`BloomCheck` 會回錯答案 — seed 插入的 key 全部變 false negative，讀取路徑直接 404。drift 會壞功能，不是「重複很醜」。

**做法**

1. 把常數（`bloomKey`、`bloomSize`、`bloomHashes`）和 `bloomPositions` 留在 `shortener/store/redis.go`，export 常數讓外部看得到。
2. `cmd/seed/main.go` 刪掉自己那份 `bloomAdd`，改呼叫 `store.NewRedisStore(...).BloomAdd(ctx, key)`。
3. 驗證：seed 完後隨機 `BloomCheck` 抽樣，必須全部回 true。

**影響檔案**

- `shortener/store/redis.go` — 常數 export
- `cmd/seed/main.go` — 刪 `bloomAdd`、刪常數、改 import

---

## 2. consumer 手寫 `contains` 換成 `strings.Contains`（順手）

**問題**

`cmd/consumer/main.go:120-131` 自己刻字串 contains。標準庫有。

**做法**

```go
import "strings"

for _, m := range []string{"Mobile", "Android", "iPhone"} {
    if strings.Contains(event.UserAgent, m) {
        device = "mobile"
        break
    }
}
```

刪掉 `contains` 和 `containsStr` 兩個函數共 12 行。

**影響檔案**

- `cmd/consumer/main.go`

---

## 3. flush 註解對不上實作（順手）

**問題**

`cmd/flush/main.go:14` 註解寫 `Runs every 30 seconds`，但實作是 `time.NewTicker(5 * time.Second)`。CLAUDE.md 也是 5s。

**做法**

改註解成 `Runs every 5 seconds`，或整段刪掉 — 常數本來就在程式碼裡。

**影響檔案**

- `cmd/flush/main.go`

---

## 4. consumer 硬寫 "TW"（要選擇）

**問題**

`cmd/consumer/main.go:89`：
```go
b.Append(r.shortKey, r.ts, r.ip, "TW", r.device)
```

ClickHouse schema 有 `country` 欄位但沒做 IP → 國家查詢。分析結果會誤導（全部顯示台灣）。

**兩個選擇**

- **選 A**：補 GeoIP lookup（例如 `github.com/oschwald/geoip2-golang` + MaxMind DB）。作品集加分，但要下載 DB 檔案。
- **選 B**：砍掉 `country` 欄位。`click_events` schema 改成不含 country，analytics 也拿掉 device 以外的地理 split。乾淨。

我建議 B。demo 不需要 GeoIP 的複雜度；要做就做全套（device + country + browser）才有意義。

**影響檔案（選 B）**

- `scripts/init_clickhouse.sql`
- `cmd/consumer/main.go`
- `shortener/store/clickhouse.go`（如果有用到 country 欄位）

---

## 5. Graceful shutdown（作品集級別）

**問題**

所有 binary 都用 `log.Fatal` 或直接 ticker loop 跑到 process 被 kill。沒接 SIGTERM、沒 cancel context、Kafka reader / PG pool 沒主動 `Close()`。

本地 demo 沒事。但給人看會被扣分 — production 寫法一定有 graceful shutdown。

**模板**

```go
ctx, cancel := signal.NotifyContext(context.Background(),
    os.Interrupt, syscall.SIGTERM)
defer cancel()

// main loop 用 ctx
for {
    select {
    case <-ctx.Done():
        log.Println("shutting down...")
        return
    case <-ticker.C:
        flush(ctx, ...)
    }
}
// defer 時會自動關 pool / reader
```

**做法**

五個 binary 都加，優先順序：

1. `cmd/shortener/main.go` — 最重要（對外服務）
2. `cmd/consumer/main.go` — 中（避免 batch 丟失）
3. `cmd/flush/main.go` — 中（避免 GetDel 後 crash 導致計數消失）
4. `cmd/seed/main.go`、`cmd/mimic/main.go` — 低（工具程式）

**影響檔案**

- `cmd/shortener/main.go`
- `cmd/consumer/main.go`
- `cmd/flush/main.go`
- (optional) `cmd/seed/main.go`、`cmd/mimic/main.go`

---

## 執行順序建議

**Phase 1：一次做完（低風險）**

- [ ] 3. flush 註解
- [ ] 2. `strings.Contains`
- [ ] 1. Bloom 抽共用

三個一起，動的檔案不重疊，可以一個 commit 或拆三個。

**Phase 2：決定方向後再做**

- [ ] 4. country 欄位 — 等決定 A/B

**Phase 3：作品集衝刺**

- [ ] 5. graceful shutdown — 五個 binary 一次到位

---

## 已知擴展性限制（不修，但要會講）

這些不是 bug — demo 規模跑得動，但 production 會爆。作品集講解時主動提，比假裝沒看到誠實。

### A. Redis 沒持久化 + bloom 沒 startup rebuild

**情境**：`docker compose down` 之後再 up，PG 50k row 還在（有 volume），Redis bloom 空了。shortener 啟動，curl seeded key → bloom miss → 404，但資料明明在 PG。

**為什麼 demo 不爆**：reset 路徑永遠是 `make db-prune && make seed`，兩步綁在一起，bloom 一定跟 PG 同步。

**Production 修法**：擇一
1. Redis 掛 volume（一行 yaml）
2. shortener startup 時 `SELECT short_key FROM url_mappings` 重灌 bloom
3. lazy backfill（bloom miss 時打一次 PG，補回 bloom）— 不推，攻擊者亂打可灌爆

### B. flush worker 單 goroutine 序列 INSERT

**問題**：flush 一輪 = SCAN + (GetDel + INSERT) × N。每筆 INSERT 走 pool 約 2ms。10k unique key = 20s，超過 5s ticker。

**會發生什麼**：
- Go `time.Ticker` channel buffer = 1，flush 跑 20s 期間，t=10、t=15 那兩次 tick 直接被丟（不會堆 goroutine 但會漏跑）。
- flush 跑期間 shortener 持續 INCR，下一輪要處理的 key 比這輪還多 → backpressure 雪崩 → Redis 記憶體膨脹直到爆。

**為什麼 demo 不爆**：`cmd/mimic/main.go` 是 80/20 Zipf，熱點 hot 1%（預設 500 key）。即使 1000 req/s，unique key 數 ≤ 500，flush 一輪 1 秒搞定。**前提是熱點集中**。改均勻分佈或熱點分散就會卡。

**Production 修法**：
1. **Pipeline GetDel**：一次發 1000 個 GetDel，10k key 從 10000 RTT 降到 10 RTT
2. **批次 UPSERT**：湊成單一 multi-row INSERT 或 `pgx.CopyFrom`，10000 round-trip → 1 個
3. **分片 worker**：多個 flush 各認領 `clicks:0*`、`clicks:1*`...
4. **改 ticker 為 self-pace**：`time.AfterFunc(flushDuration)`，自然不堆但也不丟

排序：1 + 2 是第一刀，能解 90% 案例。3 是真的需要橫向擴展時。

### C. consumer 預讀 buffer 在 prune 時是污染源

**問題**：kafka-go `MaxBytes: 10e6` 會在 process 內預先 fetch 一批訊息（上千則）。`make db-prune` 砍掉 Kafka topic 重建後，consumer 記憶體裡那批舊訊息還是會繼續寫進新的（空的）ClickHouse → analytics 圖表一開始就帶舊資料。

**Demo 解法（CLAUDE.md 已寫）**：reset 前先 Ctrl+C 停 consumer。

**Production 修法**：consumer 收到 SIGTERM 時 commit offset + drain buffer + close，搭配 graceful shutdown（Phase 3）。

### D. flush race during prune

**問題**：`db-prune` 五步循序執行，`DROP TABLE url_stats` 跟 `FLUSHALL Redis` 之間有縫。flush ticker 可能剛好觸發，GetDel 拿到 +3、INSERT url_stats 噴 error（表已 DROP）；或更糟，prune 跑完 db-init 重建表後 flush 才把舊計數寫進新表，污染。

**Demo 解法**：reset 前 Ctrl+C 停 flush。CLAUDE.md 只警告了 consumer，flush 也該警告 — 補進文件 OK 但很瑣碎。

**Production 修法**：跟 C 一樣，graceful shutdown。

---

## 不做的事

以下發現過但不打算動：

- **`shortener/store/` 搬去 `internal/store/`**：現在 store 只給 shortener 用，名字綁 shortener 是誠實的。搬過去要連邏輯一起搬才有意義，工作量太大，收益有限。
- **seed 的 `randomShortKey` 不走 counter**：是設計選擇（要能批量生不進 counter），不是 bug。
- **flush 用單一 worker SCAN**：見上面 B 段，已知限制不是要修的 bug。
