import os, json, time, uuid, socket, asyncio
import redis.asyncio as redis
from redis.exceptions import ResponseError, TimeoutError as RedisTimeoutError
import httpx

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

r = redis.from_url(REDIS_URL, decode_responses=True)


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
        data = resp.text                      # уже OpenAI-формат (вкл. tool_calls)
    else:
        data = json.dumps(_echo_full(payload))
    await r.rpush(f"llm:response:{rid}", data)
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
                    await r.xadd(key, {"type": "error", "data": resp.text[:2000]})
                else:
                    async for line in resp.aiter_lines():
                        if not line.startswith("data:"):
                            continue
                        chunk = line[5:].lstrip()
                        if chunk == "[DONE]":
                            break
                        await r.xadd(key, {"type": "chunk", "data": chunk})
        else:
            for ev in _echo_chunks(payload):
                await r.xadd(key, {"type": "chunk", "data": json.dumps(ev)})
        await r.xadd(key, {"type": "done"})
    except Exception as e:
        await r.xadd(key, {"type": "error", "data": str(e)})
    await r.expire(key, RESP_TTL)


async def process(entry_id, fields, http):
    rid = fields.get("id")
    try:
        payload = json.loads(fields["payload"])
        payload.setdefault("model", MODEL_NAME)
        if fields.get("stream") == "1":
            await handle_stream(rid, payload, http)
        else:
            await handle_unary(rid, payload, http)
    except Exception as e:                    # ядовитое сообщение не должно ронять воркер
        print(f"[err] {rid}: {e}")
        if fields.get("stream") != "1" and rid:
            err = {"error": {"message": str(e), "type": "worker_error"}}
            try:
                await r.rpush(f"llm:response:{rid}", json.dumps(err))
                await r.expire(f"llm:response:{rid}", RESP_TTL)
            except Exception:
                pass
    finally:
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
            print(f"[task-err] {task.exception()}")

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as http:
        await reclaim(http)
        print(f"воркер {CONSUMER} запущен; upstream={VLLM_BASE_URL or 'ECHO (заглушка)'}; "
              f"параллелизм={MAX_INFLIGHT}")
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
