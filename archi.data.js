// url-shortener-demo: Go + Redis + PostgreSQL + Kafka + ClickHouse.
// Source for shortener/static/archi.html (GET /archi). Rebuild after editing:
//   python3 ~/.claude/skills/hiroki-archi-flow/build.py archi.data.js shortener/static/archi.html --standalone
// Every `src` was read from the repo. Sample values (3d7, the URL, the counts) are made up;
// field names, Redis keys and SQL are the real ones.
const B = (en, zh) => ({ en, zh })
const H = 'shortener/handler/'
// The same action cites the same source in every journey it shows up in.
const SSE_SRC = H + 'redirect.go:47 → shortener/hub/hub.go:34 → ' + H + 'stream.go:27'

const K = '3d7', URL_ = 'https://example.com/articles/42', TS = '2026-11-24T09:15:02.418+08:00'
const UA = 'Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X)'
const EVENT = { short_key: K, timestamp: TS, ip: '127.0.0.1', user_agent: UA }
const FRAME = { frame: 'data: ' + K + '\n\n' }
const GETBIT = 'GETBIT bloom:urls <pos>  (5 positions, one pipeline)'
const BUCKET = { hour: '2026-11-24 09:00:00', clicks: 812 }
const DEVICES = [{ device: 'desktop', clicks: 540 }, { device: 'mobile', clicks: 272 }]
const LIVE = ['hit', 'miss']

const ARCHI = {
  title: 'URL Shortener Flow',
  viewBox: [1160, 664],

  nodes: {
    client:    { x: 30,  y: 156, w: 180, h: 72,  shape: 'pill', t: 'Visitor',    s: 'browser / mimic', icon: 'role:browser' },
    dash:      { x: 30,  y: 466, w: 180, h: 72,  shape: 'pill', t: 'Dashboard',  s: 'browser tab',     icon: 'role:browser' },
    shortener: { x: 330, y: 36,  w: 180, h: 592, shape: 'rect', t: 'shortener',  s: 'Go · :8080',      icon: 'brand:go', hue: ['#00758f', '#55d2ec'],
                 routes: ['POST /shorten', 'GET /:key', 'GET /stats', 'GET /stream', 'GET /analytics/data'] },
    // Redis does three different jobs here, one per key, so each key gets its own row and its own line.
    redis:     { x: 690, y: 36,  w: 180, h: 164, shape: 'cyl',  t: 'Redis',      s: ':6380',           icon: 'brand:redis', hue: ['#c2302a', '#ff7168'],
                 slots: [{ id: 'bloom', y: 119, t: 'bloom:urls' }, { id: 'cache', y: 147, t: 'url:<key>' }, { id: 'clicks', y: 175, t: 'clicks:<key>' }] },
    pg:        { x: 690, y: 246, w: 180, h: 72,  shape: 'cyl',  t: 'PostgreSQL', s: ':5433',           icon: 'brand:postgresql', hue: ['#2c6591', '#70b6ea'] },
    kafka:     { x: 690, y: 436, w: 180, h: 72,  shape: 'pipe', t: 'Kafka',      s: 'click-events',    icon: 'brand:apachekafka', hue: ['#17202b', '#e5eaf0'] },
    flush:     { x: 950, y: 139, w: 180, h: 72,  shape: 'hex',  t: 'flush',      s: 'Go · every 5s',   icon: 'role:clock', hue: ['#00758f', '#55d2ec'] },
    consumer:  { x: 950, y: 436, w: 180, h: 72,  shape: 'hex',  t: 'consumer',   s: 'Go · 4 writers',  icon: 'role:batch', hue: ['#00758f', '#55d2ec'] },
    ch:        { x: 950, y: 556, w: 180, h: 72,  shape: 'cyl',  t: 'ClickHouse', s: 'click_events',    icon: 'brand:clickhouse', hue: ['#8f6b00', '#f0c93a'] },
  },

  edges: {
    http:   { a: 'client', b: 'shortener', kind: 'call',   pts: [[210, 192], [330, 192]], lab: 'HTTP',                lx: 270, ly: 183 },
    dreq:   { a: 'dash', b: 'shortener',   kind: 'call',   pts: [[208, 490], [330, 490]], lab: 'GET /stats',          lx: 269, ly: 481 },
    sse:    { a: 'shortener', b: 'dash',   kind: 'stream', pts: [[330, 516], [207, 516]], lab: 'SSE +1',              lx: 269, ly: 533 },
    bloom:  { a: 'shortener', b: 'redis',  kind: 'call',   pts: [[510, 119], [690, 119]], lab: 'GETBIT · SETBIT',     lx: 600, ly: 111, slot: 'bloom' },
    cache:  { a: 'shortener', b: 'redis',  kind: 'call',   pts: [[510, 147], [690, 147]], lab: 'GET · SET',           lx: 600, ly: 139, slot: 'cache' },
    clicks: { a: 'shortener', b: 'redis',  kind: 'call',   pts: [[510, 175], [690, 175]], lab: 'INCR · GET',          lx: 600, ly: 167, slot: 'clicks' },
    pg:     { a: 'shortener', b: 'pg',     kind: 'call',   pts: [[510, 282], [690, 282]], lab: 'mappings · stats',    lx: 600, ly: 273 },
    pub:    { a: 'shortener', b: 'kafka',  kind: 'stream', pts: [[510, 472], [690, 472]], lab: 'publish click event', lx: 600, ly: 463 },
    chq:    { a: 'shortener', b: 'ch',     kind: 'call',   pts: [[510, 592], [950, 592]], lab: 'analytics queries',   lx: 730, ly: 583 },
    scan:   { a: 'redis', b: 'flush',      kind: 'tick',   pts: [[870, 175], [950, 175]], lab: 'GETDEL',              lx: 910, ly: 166, slot: 'clicks', every: 5 },
    upsert: { a: 'flush', b: 'pg',         kind: 'tick',   pts: [[1040, 211], [1040, 282], [870, 282]], lab: 'UPSERT url_stats', lx: 955, ly: 273, every: 5 },
    read:   { a: 'kafka', b: 'consumer',   kind: 'stream', pts: [[870, 472], [950, 472]], lab: 'ReadMessage',         lx: 910, ly: 463 },
    write:  { a: 'consumer', b: 'ch',      kind: 'stream', pts: [[1040, 508], [1040, 556]], lab: '500 rows or 2s',    lx: 1030, ly: 536, anchor: 'end' },
  },

  journeys: [
    // A journey with `variants` has no "if" inside its steps. The reader picks the outcome,
    // and a step tagged `v` only exists in those outcomes.
    { id: 'click', name: B('Click a short URL', '點短網址'), route: 'GET /:key', start: 'miss',
      variants: [
        { id: 'none', name: B('key does not exist', 'key 不存在') },
        { id: 'hit', name: B('cache hit', 'cache 有') },
        { id: 'miss', name: B('cache miss', 'cache 沒有') },
      ],
      steps: [
        { e: [['http', 'fwd']], src: H + 'redirect.go:22',
          t: B('The browser sends <code>GET /:key</code>.', '瀏覽器送出 <code>GET /:key</code>。'),
          d: { method: 'GET', path: '/' + K, headers: { 'User-Agent': UA } } },
        { v: ['none'], e: [['bloom', 'both']], src: H + 'redirect.go:26 · store/redis.go:102',
          t: B('shortener asks Redis for 5 bits of <code>bloom:urls</code>. One of them is 0, so this key was never created.',
               '問 Redis 的 <code>bloom:urls</code>，查 5 個 bit。其中有 0，代表這個 key 從來沒建過。'),
          d: { send: GETBIT, reply: [1, 0, 1, 1, 0] } },
        { v: ['none'], e: [['http', 'back']], src: H + 'redirect.go:28',
          t: B('It answers 404 right away. The cache, PostgreSQL and Kafka are not touched this time.',
               '直接回 404。這次 shortener 沒有去問 cache 跟 PostgreSQL，也沒有送東西進 Kafka。'),
          d: { status: 404, body: { error: 'not found' } } },
        { v: LIVE, e: [['bloom', 'both']], src: H + 'redirect.go:26 · store/redis.go:105',
          t: B('shortener asks Redis for 5 bits of <code>bloom:urls</code>. All are 1, so the key may exist and the request goes on.',
               '問 Redis 的 <code>bloom:urls</code>，查 5 個 bit。全部是 1，這個 key 可能存在，所以繼續往下。'),
          d: { send: GETBIT, reply: [1, 1, 1, 1, 1] } },
        { v: ['hit'], e: [['cache', 'both']], src: H + 'redirect.go:33',
          t: B('It asks the cache for <code>url:&lt;key&gt;</code> and gets the long_url, so PostgreSQL is not asked this time.',
               '跟 cache 要 <code>url:&lt;key&gt;</code>，拿到 long_url，所以這次不用問 PostgreSQL。'),
          d: { send: 'GET url:' + K, reply: URL_ } },
        { v: ['miss'], e: [['cache', 'both']], src: H + 'redirect.go:33 · store/redis.go:15',
          t: B('It asks the cache for <code>url:&lt;key&gt;</code> and gets nothing. The cache keeps an entry for 10 minutes, so the first click after that ends up here.',
               '跟 cache 要 <code>url:&lt;key&gt;</code>，沒有。cache 只留 10 分鐘，所以過期之後第一個來點的人會拿不到。'),
          d: { send: 'GET url:' + K, reply: null } },
        { v: ['miss'], e: [['pg', 'both']], src: H + 'redirect.go:36',
          t: B('So it asks PostgreSQL, table <code>url_mappings</code>.', '改問 PostgreSQL 的 <code>url_mappings</code>。'),
          d: { send: 'SELECT long_url FROM url_mappings WHERE short_key = $1', args: [K], reply: { long_url: URL_ } } },
        { v: ['miss'], e: [['cache', 'fwd']], src: H + 'redirect.go:42',
          t: B('It writes the long_url back to <code>url:&lt;key&gt;</code>, so the next click does not need PostgreSQL.',
               '把查到的 long_url 寫回 <code>url:&lt;key&gt;</code>，下一個來點的人就不用再問 PostgreSQL。'),
          d: { send: 'SET url:' + K + ' ' + URL_, ttl: '10m' } },
        { v: LIVE, e: [['clicks', 'fwd']], src: H + 'redirect.go:46',
          t: B('<code>INCR clicks:&lt;key&gt;</code>. The click count lives in Redis first.', '<code>INCR clicks:&lt;key&gt;</code>，點擊數先記在 Redis。'),
          d: { send: 'INCR clicks:' + K, reply: 1 } },
        { v: LIVE, e: [['sse', 'fwd']], src: SSE_SRC,
          t: B('The hub pushes this key to every browser that has the dashboard open. That row goes up by one.',
               'hub 把這個 key 推給每個開著 dashboard 的瀏覽器，那一列的數字加一。'),
          d: FRAME },
        { v: LIVE, e: [['pub', 'fwd']], src: H + 'redirect.go:48',
          t: B('It publishes the click event to the Kafka topic <code>click-events</code> and does not wait for an answer.',
               '把 click event 送進 Kafka 的 <code>click-events</code> topic，不等它回覆。'),
          d: { topic: 'click-events', value: EVENT } },
        { v: LIVE, e: [['http', 'back']], src: H + 'redirect.go:56',
          t: B('It answers 302 and the browser goes to the long_url. For the visitor, this is the end.',
               '回 302，瀏覽器轉去 long_url。對訪客來說到這裡就結束了。'),
          d: { status: 302, headers: { Location: URL_ } } },
        { v: LIVE, e: [['scan', 'fwd'], ['upsert', 'fwd']], src: 'cmd/flush/main.go:34, 53, 62, 68',
          t: B('Later, flush scans <code>clicks:*</code> every 5 seconds, takes each number with GETDEL, and adds it to <code>url_stats</code> in PostgreSQL.',
               '之後 flush 每 5 秒掃一次 <code>clicks:*</code>，用 GETDEL 把數字拿走，加進 PostgreSQL 的 <code>url_stats</code>。'),
          d: { from_redis: { send: 'GETDEL clicks:' + K, reply: 17 }, to_postgres: { table: 'url_stats', short_key: K, total_clicks: '+= 17' } } },
        { v: LIVE, e: [['read', 'fwd'], ['write', 'fwd']], src: 'cmd/consumer/main.go:27, 28, 72, 159',
          t: B('consumer reads the event from Kafka. When it has 500 rows, or 2 seconds have passed, it writes the whole batch to ClickHouse.',
               'consumer 從 Kafka 讀出那筆 event，湊滿 500 筆或等了 2 秒，就整批寫進 ClickHouse。'),
          d: { from_kafka: EVENT, to_clickhouse: { table: 'click_events', short_key: K, ts: TS, ip: '127.0.0.1', device: 'mobile' } } },
      ] },

    { id: 'shorten', name: B('Create a short URL', '建短網址'), route: 'POST /shorten', steps: [
      { e: [['http', 'fwd']], src: H + 'shorten.go:26',
        t: B('The client sends <code>POST /shorten</code> with long_url in the body.', '送出 <code>POST /shorten</code>，body 裡帶 long_url。'),
        d: { method: 'POST', path: '/shorten', body: { long_url: URL_ } } },
      { e: [['pg', 'both']], src: H + 'shorten.go:33 · shortener/keygen/counter.go:26',
        t: B('shortener takes the next number from <code>id_counter</code> in PostgreSQL and turns it into base62. That is the short_key.',
             '跟 PostgreSQL 的 <code>id_counter</code> 拿下一個號碼，轉成 base62 當 short_key。'),
        d: { send: 'UPDATE id_counter SET next_val = next_val + 1 WHERE id = 1 RETURNING next_val', reply: { next_val: 50001 }, then: 'base62(next_val) = short_key' } },
      { e: [['pg', 'fwd']], src: H + 'shorten.go:39 · shortener/store/postgres.go:26',
        t: B('It saves short_key and long_url in <code>url_mappings</code>.', '把 short_key 跟 long_url 存進 <code>url_mappings</code>。'),
        d: { send: 'INSERT INTO url_mappings (short_key, long_url) VALUES ($1, $2) ON CONFLICT DO NOTHING', args: [K, URL_] } },
      { e: [['cache', 'fwd']], src: H + 'shorten.go:45',
        t: B('It also writes a copy to the Redis cache, so the first click does not need PostgreSQL.',
             '順手寫一份到 Redis cache，所以第一次有人點的時候不用問 PostgreSQL。'),
        d: { send: 'SET url:' + K + ' ' + URL_, ttl: '10m' } },
      { e: [['bloom', 'fwd']], src: H + 'shorten.go:46',
        t: B('It adds the key to the Bloom filter. Without this, a click on the new short URL would get 404 at the Bloom filter check.',
             '把 key 加進 Bloom filter。沒加的話，點這個短網址會在問 Bloom filter 那步就拿到 404。'),
        d: { send: 'SETBIT bloom:urls <pos> 1  (5 positions, one pipeline)' } },
      { e: [['http', 'back']], src: H + 'shorten.go:48',
        t: B('It answers 201 with the short_url. Kafka and ClickHouse take no part in this path.',
             '回 201 跟 short_url。建短網址用不到 Kafka 跟 ClickHouse。'),
        d: { status: 201, body: { short_key: K, short_url: 'http://localhost:8080/' + K } } },
    ] },

    { id: 'watch', name: B('Read the numbers', '看數字'), route: 'GET /stats · /analytics/data', steps: [
      { e: [['dreq', 'fwd']], src: H + 'stats.go:24',
        t: B('The dashboard opens and sends <code>GET /stats</code>.', 'dashboard 開起來，先送 <code>GET /stats</code>。'),
        d: { method: 'GET', path: '/stats' } },
      { e: [['pg', 'both']], src: H + 'stats.go:25, 29',
        t: B('shortener reads the top 100 by clicks from <code>url_stats</code> in PostgreSQL.', '從 PostgreSQL 的 <code>url_stats</code> 拿點擊數前 100 名。'),
        d: { send: '... ORDER BY s.total_clicks DESC LIMIT 100', reply: [{ short_key: K, total_clicks: 1240, long_url: URL_ }, '... up to 100 rows'] } },
      { e: [['clicks', 'both']], src: H + 'stats.go:48',
        t: B('Clicks from the last few seconds are still in Redis, because flush has not moved them yet. So it adds <code>clicks:&lt;key&gt;</code> for each key.',
             '最近幾秒的點擊數還在 Redis，因為 flush 每 5 秒才把它們寫進 PostgreSQL 一次，所以每個 key 要再加上 <code>clicks:&lt;key&gt;</code> 的數字。'),
        d: { send: 'GET clicks:' + K, reply: 17 } },
      { e: [['dreq', 'back']], src: H + 'stats.go:52, 55',
        t: B('It sorts again by the new totals and answers.', '兩邊加起來重新排序，回給瀏覽器。'),
        d: { status: 200, body: [{ short_key: K, long_url: URL_, total_clicks: 1257 }, '...'] } },
      { e: [['sse', 'fwd']], src: SSE_SRC,
        t: B('After that the browser stays connected to <code>GET /stream</code>. Each click sends one key over SSE.',
             '之後瀏覽器掛在 <code>GET /stream</code> 上，每有人點一次，SSE 就推一個 key 過來。'),
        d: FRAME },
      { e: [['dreq', 'fwd']], src: H + 'analytics.go:26',
        t: B('The analytics page sends <code>GET /analytics/data</code>.', '切到 analytics 頁，送 <code>GET /analytics/data</code>。'),
        d: { method: 'GET', path: '/analytics/data?g=30m' } },
      { e: [['chq', 'both']], src: H + 'analytics.go:30, 36, 67',
        t: B('ClickHouse counts clicks per time bucket, the top 10 keys, and the split by device.', 'ClickHouse 算出每個時間區間的點擊數、前 10 名的 key、裝置比例。'),
        d: { reply: { hourly: [BUCKET], top: [{ short_key: K, clicks: 96 }], devices: DEVICES } } },
      { e: [['pg', 'both']], src: H + 'analytics.go:48',
        t: B('ClickHouse stores only short_key, so shortener asks PostgreSQL for the long_url of those 10 keys.',
             'ClickHouse 只存 short_key，所以前 10 名的 long_url 要回 PostgreSQL 補。'),
        d: { lookup: 'url_mappings', keys: [K, '... 10 keys'], reply: [{ short_key: K, long_url: URL_ }] } },
      { e: [['dreq', 'back']], src: H + 'analytics.go:73',
        t: B('All three sets of numbers go back to the browser in one response.', '三組數字一起回給瀏覽器畫圖。'),
        d: { status: 200, body: { hourly: [BUCKET], top: [{ short_key: K, long_url: URL_, clicks: 96 }], devices: DEVICES } } },
    ] },
  ],
}
