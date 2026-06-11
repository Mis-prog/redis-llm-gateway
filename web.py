import os, json, uuid
import redis.asyncio as redis
from redis.exceptions import TimeoutError as RedisTimeoutError
from fastapi import FastAPI, Request, HTTPException, Header
from fastapi.responses import JSONResponse, StreamingResponse

# OpenAI-совместимый фронт. Принимает /v1/chat/completions, кладёт задачу в Redis,
# ждёт ответ от воркера (server.py). tools/tool_choice идут насквозь — их разбирает
# upstream vLLM, поднятый с --enable-auto-tool-choice --tool-call-parser qwen3-coder.

REDIS_URL      = os.getenv("REDIS_URL", "redis://localhost:6379/0")
REQUEST_STREAM = os.getenv("REQUEST_STREAM", "llm:requests")
MODEL_NAME     = os.getenv("MODEL_NAME", "qwen3-coder")
GATEWAY_KEY    = os.getenv("GATEWAY_API_KEY")          # опц. bearer для клиентов
REPLY_TIMEOUT  = int(os.getenv("REPLY_TIMEOUT", "300"))

app = FastAPI(title="redis-llm-gateway")
r = redis.from_url(REDIS_URL, decode_responses=True)


def _auth(authorization):
    if GATEWAY_KEY and authorization != f"Bearer {GATEWAY_KEY}":
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

    await r.xadd(REQUEST_STREAM, {
        "id": rid,
        "stream": "1" if stream else "0",
        "payload": json.dumps(body),
    })

    if stream:
        return StreamingResponse(
            _relay(rid),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    try:
        reply = await r.blpop(f"llm:response:{rid}", timeout=REPLY_TIMEOUT)
    except RedisTimeoutError:
        reply = None
    if reply is None:
        raise HTTPException(504, "worker timeout")
    return JSONResponse(json.loads(reply[1]))            # blpop -> (key, value)


async def _relay(rid):
    """Перекладываем SSE-чанки воркера (Redis Stream) в HTTP-ответ клиента."""
    key, last = f"llm:stream:{rid}", "0"
    while True:
        try:
            res = await r.xread({key: last}, count=20, block=REPLY_TIMEOUT * 1000)
        except RedisTimeoutError:
            res = None
        if not res:
            yield 'data: {"error":{"message":"worker timeout"}}\n\n'
            yield "data: [DONE]\n\n"
            return
        for _, entries in res:
            for eid, f in entries:
                last = eid
                t = f.get("type")
                if t == "chunk":
                    yield f"data: {f['data']}\n\n"
                elif t == "done":
                    yield "data: [DONE]\n\n"
                    return
                elif t == "error":
                    yield f"data: {json.dumps({'error': {'message': f.get('data', 'error')}})}\n\n"
                    yield "data: [DONE]\n\n"
                    return
