from common.redis_client import redis_client

r_s = redis_client.lpush('m2e-test', {'a': 'a', 'b': 'b'})

for i in range(0, 10):
    r_r = redis_client.rpop('m2e-test')
    print(r_r)
