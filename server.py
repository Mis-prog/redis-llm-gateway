import os, json, time, uuid, socket, asyncio, logging, collections
import redis.asyncio as redis
from redis.exceptions import ResponseError, TimeoutError as RedisTimeoutError
import httpx
from crypto import encrypt, decrypt, ENABLED as CRYPTO_ON

# Воркер: читает задачи из Redis (consumer group) и форвардит в upstream vLLM
# (OpenAI /v1/chat/completions). tools/tool_choice идут насквозь — их разбирает vLLM
# (--enable-auto-tool-choice --tool-call-parser qwen3-coder).
# Async: один процесс держит до MAX_INFLIGHT запросов в полёте, насыщая батчинг vLLM.
# Если VLLM_BASE_URL не задан — отвечает эхо-заглушкой (тест без GPU).

REDIS_URL      = os.getenv("REDIS_URL", "redis://localhost:6379/0")
REQUEST_STREAM = os.getenv("REQUEST_STREAM", "llm:requests")
GROUP          = os.getenv("CONSUMER_GROUP", "workers")
CONSUMER       = os.getenv("CONSUMER_NAME") or f"{socket.gethostname()}-{os.getpid()}"
VLLM_BASE_URL  = os.getenv("VLLM_BASE_URL")            # напр. http://localhost:8000/v1
VLLM_API_KEY   = os.getenv("VLLM_API_KEY", "EMPTY")
MODEL_NAME     = os.getenv("MODEL_NAME", "qwen3-coder")
RESP_TTL       = int(os.getenv("RESP_TTL", "300"))
HTTP_TIMEOUT   = float(os.getenv("HTTP_TIMEOUT", "600"))
IDLE_RECLAIM   = int(os.getenv("IDLE_RECLAIM_MS", "60000"))
MAX_INFLIGHT   = int(os.getenv("MAX_INFLIGHT", "32"))  # ~ как --max-num-seqs у vLLM
HEARTBEAT_SEC  = int(os.getenv("HEARTBEAT_SEC", "10"))  # период строки-сводки в логе и метрик в Redis
REQUEST_TTL_MS = int(os.getenv("REQUEST_TTL_MS", str(int(max(HTTP_TIMEOUT, RESP_TTL, 300) * 2 + 60) * 1000)))  # TTL записей в llm:requests (XTRIM MINID)
ADMIN_HOST     = os.getenv("ADMIN_HOST", "127.0.0.1")  # admin-панель воркера (см. start_admin)
ADMIN_PORT     = int(os.getenv("ADMIN_PORT", "8090"))  # 0 → выключить

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(),
                    format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("worker")

r = redis.from_url(REDIS_URL, decode_responses=True)
log.info("шифрование Redis-payload: %s", "ВКЛ" if CRYPTO_ON else "ВЫКЛ")

STATS_KEY = f"llm:worker:stats:{CONSUMER}"


# ---------- метрики воркера (только агрегаты, без содержимого запросов) ----------

_t_start   = time.time()
_inflight  = 0
_processed = 0
_errors    = 0
_by_mode   = {"unary": 0, "stream": 0}
_lat    = collections.deque(maxlen=256)   # длительность обработки, ms
_qwait  = collections.deque(maxlen=256)   # ожидание в очереди до старта, ms
_recent = collections.deque(maxlen=512)   # время завершения задач — для расчёта rps

def _pct(samples, p):
    if not samples:
        return 0
    s = sorted(samples)
    return s[max(0, min(len(s) - 1, round(p / 100 * (len(s) - 1))))]

def _queue_wait_ms(entry_id):
    try:                                   # ID записи стрима = "<ms>-<seq>"
        return max(0, int(time.time() * 1000) - int(str(entry_id).split("-")[0]))
    except Exception:
        return 0

def _snapshot():
    now = time.time()
    rps = round(sum(1 for t in _recent if now - t <= 60) / 60, 2)
    return {
        "consumer": CONSUMER, "upstream": VLLM_BASE_URL or "echo",
        "uptime_s": int(now - _t_start),
        "inflight": _inflight, "max_inflight": MAX_INFLIGHT,
        "processed": _processed, "errors": _errors,
        "unary": _by_mode["unary"], "stream": _by_mode["stream"],
        "rps_1m": rps,
        "lat_ms_p50": _pct(_lat, 50), "lat_ms_p95": _pct(_lat, 95),
        "qwait_ms_p50": _pct(_qwait, 50), "qwait_ms_p95": _pct(_qwait, 95),
        "ts": int(now),
    }

async def heartbeat():
    """Раз в HEARTBEAT_SEC: строка-сводка в лог + публикация метрик в Redis (с TTL)."""
    while True:
        await asyncio.sleep(HEARTBEAT_SEC)
        snap = _snapshot()
        try:
            snap["backlog"] = await r.xlen(REQUEST_STREAM)
        except Exception:
            snap["backlog"] = -1
        log.info("📊 inflight=%d/%d done=%d err=%d rps=%.1f lat_p50=%dms lat_p95=%dms qwait_p50=%dms backlog=%d",
                 snap["inflight"], snap["max_inflight"], snap["processed"], snap["errors"],
                 snap["rps_1m"], snap["lat_ms_p50"], snap["lat_ms_p95"],
                 snap["qwait_ms_p50"], snap["backlog"])
        try:
            await r.set(STATS_KEY, json.dumps(snap), ex=HEARTBEAT_SEC * 3)
        except Exception:
            pass
        try:
            cutoff = f"{int(time.time() * 1000) - REQUEST_TTL_MS}-0"
            trimmed = await r.xtrim(REQUEST_STREAM, minid=cutoff, approximate=True)
            if trimmed:
                log.info("🧹 xtrim: удалено %d записей старше %dс", trimmed, REQUEST_TTL_MS // 1000)
        except Exception:
            pass


# ---------- admin-панель воркера (лёгкий HTTP прямо в event loop) ----------

async def _admin_stats():
    """Срез для admin-панели: свои live-метрики + флот из Redis + backlog/группы/последние запросы."""
    try:
        backlog = await r.xlen(REQUEST_STREAM)
    except Exception:
        backlog = None
    groups = []
    try:
        for g in await r.xinfo_groups(REQUEST_STREAM):
            groups.append({"name": g.get("name"), "consumers": g.get("consumers"),
                           "pending": g.get("pending"), "lag": g.get("lag")})
    except Exception:
        pass
    workers = {}                               # весь флот — из опубликованных снапшотов
    try:
        async for k in r.scan_iter(match="llm:worker:stats:*"):
            v = await r.get(k)
            if v:
                try:
                    w = json.loads(v); workers[w.get("consumer")] = w
                except Exception:
                    pass
    except Exception:
        pass
    workers[CONSUMER] = {**_snapshot(), "backlog": backlog}   # свои — вживую, не из Redis
    workers = sorted(workers.values(), key=lambda w: w.get("consumer", ""))
    recent = []                                # последние запросы, расшифрованные на лету
    try:
        for eid, f in await r.xrevrange(REQUEST_STREAM, count=15):
            item = {"id": f.get("id"), "stream": f.get("stream") == "1",
                    "ts": int(str(eid).split("-")[0]) // 1000}
            try:
                body = json.loads(decrypt(f["payload"]))
                item["model"] = body.get("model")
                item["nmsg"] = len(body.get("messages", []))
                item["text"] = " ".join(_last_user(body).split())[:220]
            except Exception:
                item["text"] = "‹не удалось расшифровать›"
            recent.append(item)
    except Exception:
        pass
    return {"ts": int(time.time()), "model": MODEL_NAME, "backlog": backlog,
            "groups": groups, "workers": workers, "recent": recent}


async def _admin_handle(reader, writer):
    try:
        req_line = await reader.readline()
        if not req_line:
            return
        parts = req_line.decode("latin1").split()
        path = (parts[1] if len(parts) >= 2 else "/").split("?", 1)[0]
        while True:                            # дочитываем заголовки до пустой строки
            h = await reader.readline()
            if h in (b"\r\n", b"\n", b""):
                break
        if path == "/stats":
            payload = json.dumps(await _admin_stats(), ensure_ascii=False).encode("utf-8")
            status, ctype = "200 OK", "application/json; charset=utf-8"
        elif path in ("/", "/dashboard", "/admin"):
            payload, status, ctype = _ADMIN_HTML.encode("utf-8"), "200 OK", "text/html; charset=utf-8"
        else:
            payload, status, ctype = b"not found", "404 Not Found", "text/plain; charset=utf-8"
        writer.write((f"HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\n"
                      f"Content-Length: {len(payload)}\r\nCache-Control: no-cache\r\n"
                      f"Connection: close\r\n\r\n").encode("latin1") + payload)
        await writer.drain()
    except Exception:
        pass
    finally:
        try:
            writer.close()
        except Exception:
            pass


async def start_admin():
    if ADMIN_PORT <= 0:
        log.info("admin-панель воркера выключена (ADMIN_PORT=0)")
        return None
    try:
        srv = await asyncio.start_server(_admin_handle, ADMIN_HOST, ADMIN_PORT)
        log.info("admin-панель воркера: http://%s:%d", ADMIN_HOST, ADMIN_PORT)
        return srv
    except Exception as e:
        log.warning("admin-панель не поднялась на %s:%d (%s)", ADMIN_HOST, ADMIN_PORT, e)
        return None


# ---------- эхо-заглушка (когда нет vLLM) ----------

def _last_user(payload):
    for m in reversed(payload.get("messages", [])):
        if m.get("role") == "user":
            c = m.get("content")
            if isinstance(c, str):
                return c
            if isinstance(c, list):           # мультимодальный content
                return " ".join(p.get("text", "") for p in c if isinstance(p, dict))
    return ""

def _echo_full(payload):
    return {
        "id": f"chatcmpl-{uuid.uuid4().hex}",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": payload.get("model", MODEL_NAME),
        "choices": [{"index": 0,
                     "message": {"role": "assistant", "content": f"эхо: {_last_user(payload)}"},
                     "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
    }

def _echo_chunks(payload):
    cid, created = f"chatcmpl-{uuid.uuid4().hex}", int(time.time())
    model = payload.get("model", MODEL_NAME)
    def frame(delta, finish=None):
        return {"id": cid, "object": "chat.completion.chunk", "created": created,
                "model": model, "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]}
    yield frame({"role": "assistant"})
    for tok in f"эхо: {_last_user(payload)}".split(" "):
        yield frame({"content": tok + " "})
    yield frame({}, "stop")


# ---------- обработка одного запроса ----------

async def handle_unary(rid, payload, http):
    if VLLM_BASE_URL:
        resp = await http.post(f"{VLLM_BASE_URL}/chat/completions",
                               json={**payload, "stream": False},
                               headers={"Authorization": f"Bearer {VLLM_API_KEY}"})
        if resp.status_code != 200:
            log.warning("vLLM %s на unary %s: %s", resp.status_code, rid, resp.text[:200])
        data = resp.text                      # уже OpenAI-формат (вкл. tool_calls)
    else:
        data = json.dumps(_echo_full(payload), ensure_ascii=False)
    await r.rpush(f"llm:response:{rid}", encrypt(data))
    await r.expire(f"llm:response:{rid}", RESP_TTL)


async def handle_stream(rid, payload, http):
    key = f"llm:stream:{rid}"
    try:
        if VLLM_BASE_URL:
            async with http.stream("POST", f"{VLLM_BASE_URL}/chat/completions",
                                   json={**payload, "stream": True},
                                   headers={"Authorization": f"Bearer {VLLM_API_KEY}"}) as resp:
                if resp.status_code != 200:
                    await resp.aread()
                    log.warning("vLLM %s на stream %s: %s", resp.status_code, rid, resp.text[:200])
                    await r.xadd(key, {"type": "error", "data": encrypt(resp.text[:2000])})
                else:
                    async for line in resp.aiter_lines():
                        if not line.startswith("data:"):
                            continue
                        chunk = line[5:].lstrip()
                        if chunk == "[DONE]":
                            break
                        await r.xadd(key, {"type": "chunk", "data": encrypt(chunk)})
        else:
            for ev in _echo_chunks(payload):
                await r.xadd(key, {"type": "chunk", "data": encrypt(json.dumps(ev, ensure_ascii=False))})
        await r.xadd(key, {"type": "done"})
    except Exception as e:
        log.exception("stream %s упал", rid)
        await r.xadd(key, {"type": "error", "data": encrypt(str(e))})
    await r.expire(key, RESP_TTL)


async def process(entry_id, fields, http):
    global _inflight, _processed, _errors
    rid = fields.get("id")
    mode = "stream" if fields.get("stream") == "1" else "unary"
    qwait = _queue_wait_ms(entry_id)
    _qwait.append(qwait)
    _inflight += 1
    t0 = time.monotonic()
    log.info("→ %s %s (в очереди %dms)", mode, rid, qwait)
    try:
        payload = json.loads(decrypt(fields["payload"]))
        payload.setdefault("model", MODEL_NAME)
        if mode == "stream":
            await handle_stream(rid, payload, http)
        else:
            await handle_unary(rid, payload, http)
        dur = time.monotonic() - t0
        _lat.append(int(dur * 1000)); _by_mode[mode] += 1
        _processed += 1; _recent.append(time.time())
        log.info("✓ %s %s за %.2fс", mode, rid, dur)
    except Exception as e:                    # ядовитое сообщение не должно ронять воркер
        _errors += 1
        log.exception("✗ %s %s", mode, rid)
        if mode != "stream" and rid:
            err = {"error": {"message": str(e), "type": "worker_error"}}
            try:
                await r.rpush(f"llm:response:{rid}", encrypt(json.dumps(err, ensure_ascii=False)))
                await r.expire(f"llm:response:{rid}", RESP_TTL)
            except Exception:
                pass
    finally:
        _inflight -= 1
        await r.xack(REQUEST_STREAM, GROUP, entry_id)   # ack по завершении именно этой задачи


# ---------- запуск / восстановление ----------

async def ensure_group():
    try:
        await r.xgroup_create(REQUEST_STREAM, GROUP, id="0", mkstream=True)
    except ResponseError as e:
        if "BUSYGROUP" not in str(e):         # группа уже создана — это норм
            raise

async def reclaim(http):
    # забираем «зависшие» сообщения от упавших воркеров (PEL); путь редкий — идём последовательно
    start = "0-0"
    while True:
        try:
            res = await r.xautoclaim(REQUEST_STREAM, GROUP, CONSUMER,
                                     min_idle_time=IDLE_RECLAIM, start_id=start, count=10)
        except Exception:
            return                            # старый redis/без xautoclaim — пропускаем
        next_start, claimed = res[0], res[1]
        if claimed:
            log.info("reclaim: подобрано %d зависших сообщений", len(claimed))
        for entry_id, fields in claimed:
            if fields:
                await process(entry_id, fields, http)
        if next_start in ("0-0", "0"):
            return
        start = next_start


async def main():
    await ensure_group()
    sem = asyncio.Semaphore(MAX_INFLIGHT)

    def _done(task):
        sem.release()                         # освобождаем слот, когда задача завершилась
        if not task.cancelled() and task.exception():
            log.error("task-err: %s", task.exception())

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as http:
        await reclaim(http)
        asyncio.create_task(heartbeat())      # фоновая публикация метрик/сводок
        admin_srv = await start_admin()       # admin-панель воркера (живёт, пока жив loop)
        log.info("воркер %s запущен; upstream=%s; параллелизм=%d; сводка каждые %dс",
                 CONSUMER, VLLM_BASE_URL or "ECHO (заглушка)", MAX_INFLIGHT, HEARTBEAT_SEC)
        while True:
            try:
                resp = await r.xreadgroup(GROUP, CONSUMER, {REQUEST_STREAM: ">"},
                                          count=MAX_INFLIGHT, block=5000)
            except RedisTimeoutError:
                continue                  # блокирующий read без новых сообщений — это норма
            for _, entries in resp or []:
                for entry_id, fields in entries:
                    await sem.acquire()       # тормозим чтение, пока нет свободного слота
                    task = asyncio.create_task(process(entry_id, fields, http))
                    task.add_done_callback(_done)


_ADMIN_HTML = """<!doctype html>
<html lang="ru"><head><meta charset="utf-8">
<title>redis-llm-gateway · worker admin</title>
<style>
  :root{color-scheme:dark}
  *{box-sizing:border-box}
  body{margin:0;font:14px/1.45 -apple-system,Segoe UI,Roboto,sans-serif;background:#0d1117;color:#e6edf3}
  header{padding:16px 24px;border-bottom:1px solid #21262d;display:flex;align-items:center;gap:12px}
  h1{font-size:16px;margin:0;font-weight:600}
  .dot{width:9px;height:9px;border-radius:50%;background:#3fb950;box-shadow:0 0 8px #3fb950}
  .dot.stale{background:#d29922;box-shadow:0 0 8px #d29922}
  .muted{color:#7d8590;font-size:13px}
  main{padding:24px;max-width:1120px;margin:0 auto}
  .cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-bottom:28px}
  .card{background:#161b22;border:1px solid #21262d;border-radius:10px;padding:14px 16px}
  .card .k{color:#7d8590;font-size:11px;text-transform:uppercase;letter-spacing:.05em}
  .card .v{font-size:27px;font-weight:600;margin-top:6px;font-variant-numeric:tabular-nums}
  .card .v.warn{color:#f85149}
  table{width:100%;border-collapse:collapse;margin-bottom:28px}
  th,td{text-align:left;padding:8px 10px;border-bottom:1px solid #21262d}
  th{color:#7d8590;font-weight:500;font-size:11px;text-transform:uppercase;letter-spacing:.04em}
  td.num{font-variant-numeric:tabular-nums}
  h2{font-size:12px;color:#7d8590;text-transform:uppercase;letter-spacing:.05em;margin:0 0 10px}
  code{background:#161b22;padding:1px 6px;border-radius:5px}
  .empty{color:#7d8590;padding:14px;background:#161b22;border:1px dashed #30363d;border-radius:8px}
</style></head>
<body>
<header><span class="dot" id="dot"></span><h1>worker · admin</h1><span class="muted" id="meta">…</span></header>
<main>
  <div class="cards" id="cards"></div>
  <h2>Последние запросы · расшифровано</h2><div id="requests"></div>
  <h2>Воркеры (флот)</h2><div id="workers"></div>
  <h2>Consumer-группы</h2><div id="groups"></div>
</main>
<script>
const $=id=>document.getElementById(id);
const esc=s=>String(s).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const card=(k,v,warn)=>`<div class="card"><div class="k">${k}</div><div class="v ${warn?'warn':''}">${v}</div></div>`;
const up=s=>s<60?s+'с':s<3600?Math.floor(s/60)+'м':Math.floor(s/3600)+'ч '+Math.floor(s%3600/60)+'м';
async function tick(){
  let d;
  try{ d=await (await fetch('/stats')).json(); }
  catch(e){ $('dot').classList.add('stale'); $('meta').textContent='нет связи с воркером'; return; }
  $('dot').classList.remove('stale');
  const W=d.workers||[], sum=f=>W.reduce((a,w)=>a+(w[f]||0),0);
  $('meta').innerHTML=`модель <b>${d.model}</b> · обновлено ${new Date(d.ts*1000).toLocaleTimeString()}`;
  $('cards').innerHTML=
      card('Очередь', d.backlog??'—', (d.backlog||0)>50)
    + card('Воркеры', W.length, W.length===0)
    + card('In-flight', sum('inflight'))
    + card('Обработано', sum('processed'))
    + card('Ошибки', sum('errors'), sum('errors')>0)
    + card('RPS · 1м', W.reduce((a,w)=>a+(w.rps_1m||0),0).toFixed(1));
  const R=d.recent||[];
  $('requests').innerHTML = R.length ? `<table><tr>
      <th>время</th><th>модель</th><th>тип</th><th>сообщений</th><th>текст · расшифровано</th></tr>` +
      R.map(x=>`<tr>
        <td class="num">${new Date(x.ts*1000).toLocaleTimeString()}</td>
        <td>${esc(x.model||'—')}</td>
        <td>${x.stream?'stream':'unary'}</td>
        <td class="num">${x.nmsg??'—'}</td>
        <td>${esc(x.text||'')}</td></tr>`).join('') + `</table>`
    : `<div class="empty">Пока нет запросов.</div>`;
  $('workers').innerHTML = W.length ? `<table><tr>
      <th>consumer</th><th>upstream</th><th>uptime</th><th>in-flight</th><th>done</th><th>err</th>
      <th>rps</th><th>lat p50/p95</th><th>qwait p50/p95</th></tr>` +
      W.map(w=>`<tr>
        <td>${w.consumer}</td><td>${w.upstream}</td><td class="num">${up(w.uptime_s||0)}</td>
        <td class="num">${w.inflight}/${w.max_inflight}</td><td class="num">${w.processed}</td>
        <td class="num">${w.errors}</td><td class="num">${(w.rps_1m||0).toFixed(1)}</td>
        <td class="num">${w.lat_ms_p50}/${w.lat_ms_p95} ms</td>
        <td class="num">${w.qwait_ms_p50}/${w.qwait_ms_p95} ms</td></tr>`).join('') + `</table>`
    : `<div class="empty">Нет активных воркеров.</div>`;
  const G=d.groups||[];
  $('groups').innerHTML = G.length ? `<table><tr><th>группа</th><th>consumers</th><th>pending</th><th>lag</th></tr>` +
      G.map(g=>`<tr><td>${g.name}</td><td class="num">${g.consumers}</td>
        <td class="num">${g.pending}</td><td class="num">${g.lag??'—'}</td></tr>`).join('') + `</table>`
    : `<div class="empty">Очередь ещё не создана.</div>`;
}
tick(); setInterval(tick, 2000);
</script>
</body></html>"""


if __name__ == "__main__":
    asyncio.run(main())
