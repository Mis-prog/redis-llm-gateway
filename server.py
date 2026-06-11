import redis, json

r = redis.Redis(host="<host>", port=6379, db=0, decode_responses=True)
STREAM, GROUP, CONSUMER = "llm:requests", "workers", "worker-1"

try:
    r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
except redis.ResponseError:
    pass  # группа уже создана

print("воркер запущен, жду запросы...")
while True:
    resp = r.xreadgroup(GROUP, CONSUMER, {STREAM: ">"}, count=1, block=5000)
    if not resp:
        continue
    for _, entries in resp:
        for entry_id, fields in entries:
            request_id = fields["id"]
            payload = json.loads(fields["payload"])

            # === здесь будет vLLM, пока заглушка ===
            answer = {"text": f"эхо: {payload['prompt']}"}

            r.rpush(f"llm:response:{request_id}", json.dumps(answer))
            r.expire(f"llm:response:{request_id}", 300)   # чтоб ключи не копились
            r.xack(STREAM, GROUP, entry_id)