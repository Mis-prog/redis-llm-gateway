import os, json, uuid, time, logging

from keys import REQUEST_STREAM, resp_key, stream_key  # импорт keys грузит .env до crypto
import redis.asyncio as redis
from redis.exceptions import TimeoutError as RedisTimeoutError
from fastapi import FastAPI, Request, HTTPException, Header
from fastapi.responses import JSONResponse, StreamingResponse
from crypto import encrypt, decrypt, ENABLED as CRYPTO_ON

# OpenAI-совместимый фронт. Принимает /v1/chat/completions, кладёт задачу в Redis,
# ждёт ответ от воркера (server.py). tools/tool_choice идут насквозь — их разбирает
# upstream vLLM, поднятый с --enable-auto-tool-choice --tool-call-parser qwen3-coder.
# Наблюдаемость/admin живёт на воркере (server.py, ADMIN_PORT), а не здесь.

REDIS_URL      = os.getenv("REDIS_URL", "redis://localhost:6379/0")
MODEL_NAME     = os.getenv("MODEL_NAME", "qwen3-coder")
GATEWAY_KEY    = os.getenv("GATEWAY_API_KEY")          # опц. bearer для клиентов
REPLY_TIMEOUT  = int(os.getenv("REPLY_TIMEOUT", "300"))
STREAM_MAXLEN  = int(os.getenv("STREAM_MAXLEN", "10000"))

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(),
                    format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("gateway")

app = FastAPI(title="redis-llm-gateway")
r = redis.from_url(REDIS_URL, decode_responses=True)
log.info("шифрование Redis-payload: %s", "ВКЛ" if CRYPTO_ON else "ВЫКЛ")


def _auth(authorization):
    if GATEWAY_KEY and authorization != f"Bearer {GATEWAY_KEY}":
        log.warning("401: невалидный api key")
        raise HTTPException(401, "invalid api key")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/v1/models")
async def models(authorization: str | None = Header(None)):
    _auth(authorization)
    return {"object": "list",
            "data": [{"id": MODEL_NAME, "object": "model", "owned_by": "redis-llm-gateway"}]}


@app.post("/v1/chat/completions")
async def chat_completions(request: Request, authorization: str | None = Header(None)):
    _auth(authorization)
    body = await request.json()
    rid = str(uuid.uuid4())
    stream = bool(body.get("stream", False))
    t0 = time.monotonic()
    log.info("→ %s %s model=%s", "stream" if stream else "unary", rid,
             body.get("model", MODEL_NAME))

    await r.xadd(REQUEST_STREAM, {
        "id": rid,                               # маршрутные поля — в открытом виде
        "stream": "1" if stream else "0",
        "payload": encrypt(json.dumps(body)),    # тело запроса (messages) — шифртекст
    }, maxlen=STREAM_MAXLEN, approximate=True)   # подрезаем поток — иначе растёт вечно

    if stream:
        return StreamingResponse(
            _relay(rid, t0),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    try:
        reply = await r.blpop(resp_key(rid), timeout=REPLY_TIMEOUT)
    except RedisTimeoutError:
        reply = None
    if reply is None:
        log.warning("✗ unary %s: воркер не ответил за %dс", rid, REPLY_TIMEOUT)
        raise HTTPException(504, "worker timeout")
    log.info("✓ unary %s за %.2fс", rid, time.monotonic() - t0)
    return JSONResponse(json.loads(decrypt(reply[1])))   # blpop -> (key, value)


async def _relay(rid, t0):
    """Перекладываем SSE-чанки воркера (Redis Stream) в HTTP-ответ клиента."""
    key, last = stream_key(rid), "0"
    while True:
        try:
            res = await r.xread({key: last}, count=20, block=REPLY_TIMEOUT * 1000)
        except RedisTimeoutError:
            res = None
        if not res:
            log.warning("✗ stream %s: воркер не ответил за %dс", rid, REPLY_TIMEOUT)
            yield 'data: {"error":{"message":"worker timeout"}}\n\n'
            yield "data: [DONE]\n\n"
            return
        for _, entries in res:
            for eid, f in entries:
                last = eid
                t = f.get("type")
                if t == "chunk":
                    yield f"data: {decrypt(f['data'])}\n\n"
                elif t == "done":
                    log.info("✓ stream %s за %.2fс", rid, time.monotonic() - t0)
                    yield "data: [DONE]\n\n"
                    return
                elif t == "error":
                    err = decrypt(f["data"]) if "data" in f else "error"
                    log.warning("✗ stream %s: %s", rid, err[:200])
                    yield f"data: {json.dumps({'error': {'message': err}})}\n\n"
                    yield "data: [DONE]\n\n"
                    return
