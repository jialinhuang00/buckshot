# Refactor Plan

掃過 `cmd/*/main.go` 和 `shortener/store/*` 之後整理的待辦。按嚴重度排序。

> **狀態**：1–8 全部完成。剩可選的 GeoIP / MaxOpenConns 微調。

---

## 1. ✅ Bloom filter 邏輯抽共用（必做）

**完成於** `7a329b5`

Bloom filter 的 hash → bit position 演算法在兩個地方各寫了一份：

- `shortener/store/redis.go::bloomPositions` + `BloomAdd` + `BloomCheck`
- `cmd/seed/main.go::bloomAdd`

兩邊都硬寫 `bloomKey = "bloom:urls"`、`bloomSize = 1_000_000`、`bloomHashes = 5`，還有一模一樣的 SHA-256 → bit position 邏輯。

**風險**：drift 會讓 `BloomCheck` 回錯答案，seed 插入的 key 全部變 false negative，redirect 路徑直接 404。不是「重複很醜」，是「壞功能」。

**做法**：常數和 `bloomPositions` 留在 `shortener/store/redis.go`，seed 改呼叫 `store.RedisStore.BloomAdd`。順便加 `Ping`。

**驗證**：seed 完 `BITCOUNT bloom:urls` = 221k（50k key × 5 hash 扣碰撞），seeded key 全部 302，亂打 key 全部 404。

---

## 2. ✅ consumer 手寫 `contains` 換成 `strings.Contains`（順手）

**完成於** `20386e9`（隨 graceful shutdown 一起進）

`cmd/consumer/main.go` 自己刻字串 contains。換成 `strings.Contains`，刪 12 行手刻 helper。

---

## 3. ✅ flush 註解對不上實作（順手）

**完成於** `20386e9`

`cmd/flush/main.go` 註解寫 `Runs every 30 seconds`，實作是 5s。改成 5s。

---

## 4. ✅ consumer 硬寫 "TW" → 砍 country 欄位

**完成於** `b447b55`（選 B）

ClickHouse schema 有 `country` 欄位但沒做 IP → 國家查詢。改 schema 砍欄位，比硬寫 "TW" 誠實。也順便把 `db-prune` 的 CH 操作從 TRUNCATE 改 DROP，未來 schema 變更會自動生效。

選 A（補 GeoIP）暫不做 — demo 不需要這個複雜度，要做就做全套。

---

## 5. ✅ Graceful shutdown（作品集級別）

**完成於** `20386e9`

五個 binary 全加 `signal.NotifyContext` + drain pattern：

- **shortener**: `http.Server.Shutdown(10s timeout)`，等 in-flight redirect 跑完再退；最後 close PG / Redis / Kafka / CH。
- **consumer**: reader goroutine `ReadMessage(ctx)`，`defer close(rows)`；main loop 看到 channel close → final flush L3 → return。
- **flush**: select on `ctx.Done` → final SCAN+GetDel+UPSERT 用 `Background ctx`（不被 cancel）→ exit。
- **seed**: 雙層 ctx 檢查，中斷時 log total inserted。
- **mimic**: semaphore send + HTTP request 都帶 ctx；`outer:` label 跳出後 `wg.Wait` drain。

**順便發現的坑**：`go run` 的 parent / child orphan 問題。`pkill -f "cmd/X"` 只 match parent，殺不到實際 binary。Phase 3 之前 process 1ms 內就死所以看不到，graceful 後 child 真的需要時間 drain 才暴露這個 race。`make down` 用 `go-build.*<binary>` pattern 解決。

對照學習材料：`/tmp/graceful-demo/{ungraceful,graceful}.go`。

---

## 6. ✅ Consumer fan-out parallel writers（效能）

**完成於** `23cd16d`

**問題**：單 goroutine batch+write 約 600 events/sec。每個 `PrepareBatch+Send` 約 1s，瓶頸是序列寫入延遲，不是 CPU。

**做法**：reader → rows → batcher → batchCh → 4 個 writer goroutine。每個 batch 派給空閒的 writer，CH 接受並發 INSERT（每個變一個 part，後台 merge）。

**實測**：~1500 events/sec(2.5x)。沒到滿 4x 因為:
- Kafka 單 partition → reader 序列拉
- `clickhouse.Conn` 共享 → driver 內部排隊
- Mac 上 docker CH 的 ingest 上限

**fan-out pattern 的精髓**：channel 多 receiver 自動分配工作。`for batch := range batchCh` 同時跑 N 個 goroutine，runtime 隨機選一個拿到 batch，互斥保證、無需 mutex。

要再快只能往 (a) Kafka 多 partition + 多 consumer instance、(b) `MaxOpenConns: 8` 撐更多並行連線、(c) async insert 走 server side batching。已超 demo 範圍。

---

## 7. ✅ `make down` — graceful kill helper

**完成於** `23cd16d`

`go run ./cmd/X/...` 會起 parent + child 兩個 process。Ctrl+C 在 terminal 兩個都收到 SIGINT 沒問題，但 `pkill -f "cmd/X"` 只 match parent — child 永遠殺不到。

```makefile
down:
    SIGTERM 所有 go-build.*(shortener|consumer|flush|mimic|seed)
    sleep 5
    SIGKILL stragglers
```

不影響 docker infra。reset 流程現在是 `make down && make db-prune && ...`。

---

## 8. ✅ Dashboard SSE throttle

**完成於** `1fff729`

**問題**：mimic 1000~3000 req/s 時 `/stream` 每個事件推 browser，DOM 重繪頻率 > 顯示卡能力 → 瀏覽器 freeze。

**做法**：client-side 攢 100ms 才重繪一次。`onmessage` 只 bump `pending[key]`，`setInterval` 每 100ms flush 到 DOM。DOM mutation 從 1000+/sec 降到 10/sec。

---

## 待辦（可選）

### 🔄 ClickHouse `MaxOpenConns: 8`（小優化）

`clickhouse.Open` 加 `MaxOpenConns: 8, MaxIdleConns: 4`，可能能再推 throughput 200-500/sec。投資小，效果未必大。

### 🔄 GeoIP（可選）

如果哪天想做完整地理分析，加回 country 欄位 + `oschwald/geoip2-golang` + MaxMind DB。需要下載 ~70MB 資料庫檔。

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
3. **分片 worker**：多個 flush 各認領 `clicks:0*`、`clicks:1*`...（跟 #6 consumer 同樣的 fan-out pattern，套到 flush）
4. **改 ticker 為 self-pace**：`time.AfterFunc(flushDuration)`，自然不堆但也不丟

排序：1 + 2 是第一刀，能解 90% 案例。3 是真的需要橫向擴展時。

> 🔗 #6 consumer 已經做了類似 #3 的 fan-out。flush 還沒。如果 mimic 改均勻分佈打爆 flush，就是套同一招。

### C. consumer 預讀 buffer 在 prune 時是污染源（已大幅緩解）

**問題**：kafka-go `MaxBytes: 10e6` 會在 process 內預先 fetch 一批訊息（上千則）。`make db-prune` 砍掉 Kafka topic 重建後，consumer 記憶體裡那批舊訊息還是會繼續寫進新的（空的）ClickHouse。

**現況（Phase 3 後）**：consumer 收到 SIGTERM 走 graceful drain，L1+L2+L3 buffer 排空寫進 CH 之後才退。`make down` + `make db-prune` 流程下，Kafka topic 砍之前 consumer 已經乾淨退出，**不會污染**。

**仍有風險的情境**：跳過 `make down`，直接 `make db-prune` 而 consumer 還活著 → 老問題還在。CLAUDE.md 警告維持有效。

### D. flush race during prune（已大幅緩解）

跟 C 同模式：Phase 3 graceful shutdown + `make down` 流程已經處理。`db-prune` 期間如果 consumer / flush 還活著就還是會撞，但標準 reset 流程不會發生。

---

## 不做的事

以下發現過但不打算動：

- **`shortener/store/` 搬去 `internal/store/`**：現在 store 只給 shortener 用，名字綁 shortener 是誠實的。搬過去要連邏輯一起搬才有意義，工作量太大，收益有限。
- **seed 的 `randomShortKey` 不走 counter**：是設計選擇（要能批量生不進 counter），不是 bug。
- **flush 用單一 worker SCAN**：見上面 B 段，已知限制不是要修的 bug。需要時抄 #6 consumer 的 fan-out 套上去。
