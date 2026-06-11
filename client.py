import redis, json, uuid

r = redis.Redis(host="<host>", port=6379, db=0, decode_responses=True)

def ask(prompt, timeout=60):
    request_id = str(uuid.uuid4())
    r.xadd("llm:requests", {"id": request_id, "payload": json.dumps({"prompt": prompt})})
    res = r.blpop(f"llm:response:{request_id}", timeout=timeout)  # ждём свой ответ
    if res is None:
        raise TimeoutError("воркер не ответил")
    _, answer = res
    return json.loads(answer)

print(ask("Привет, кто ты?"))