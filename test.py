import redis 

r = redis.from_url("redis://localhost:6379", decode_responses=True)
r.set("foo", "foo", ex=10) 

print(r.ping())