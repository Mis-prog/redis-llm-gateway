import os, json, time, uuid, socket, asyncio, logging, collections

from keys import REQUEST_STREAM, resp_key, stream_key, stats_key  # импорт keys грузит .env до crypto
import redis.asyncio as redis
from redis.exceptions import ResponseError, TimeoutError as RedisTimeoutError
import httpx
from cryptography.fernet import InvalidToken
from crypto import encrypt, decrypt, ENABLED as CRYPTO_ON

# Воркер: читает задачи из Redis (consumer group) и форвардит в upstream vLLM
# (OpenAI /v1/chat/completions). tools/tool_choice идут насквозь — их разбирает vLLM
# (--enable-auto-tool-choice --tool-call-parser qwen3-coder).
# Async: один процесс держит до MAX_INFLIGHT запросов в полёте, насыщая батчинг vLLM.
# Если VLLM_BASE_URL не задан — отвечает эхо-заглушкой (тест без GPU).

REDIS_URL      = os.getenv("REDIS_URL", "redis://localhost:6379/0")
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
REQUEST_TTL_MS = int(os.getenv("REQUEST_TTL_MS", str(int(max(HTTP_TIMEOUT, RESP_TTL, 300) * 2 + 60) * 1000)))  # TTL записей в стриме (XTRIM MINID)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(),
                    format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("worker")

r = redis.from_url(REDIS_URL, decode_responses=True)
log.info("шифрование Redis-payload: %s", "ВКЛ" if CRYPTO_ON else "ВЫКЛ")

STATS_KEY = stats_key(CONSUMER)


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
    await r.rpush(resp_key(rid), encrypt(data))
    await r.expire(resp_key(rid), RESP_TTL)


async def handle_stream(rid, payload, http):
    key = stream_key(rid)
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
    except InvalidToken:                      # payload не расшифровать нашим ключом
        _errors += 1
        log.error("✗ %s %s: InvalidToken — payload зашифрован ДРУГИМ GATEWAY_CRYPTO_KEY "
                  "(чужое сообщение из общего стрима или рассинхрон ключа фронт↔воркер). "
                  "Задай уникальный KEY_PREFIX и общий GATEWAY_CRYPTO_KEY в .env.", mode, rid)
    except Exception as e:                    # ядовитое сообщение не должно ронять воркер
        _errors += 1
        log.exception("✗ %s %s", mode, rid)
        if mode != "stream" and rid:
            err = {"error": {"message": str(e), "type": "worker_error"}}
            try:
                await r.rpush(resp_key(rid), encrypt(json.dumps(err, ensure_ascii=False)))
                await r.expire(resp_key(rid), RESP_TTL)
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


if __name__ == "__main__":
    asyncio.run(main())
