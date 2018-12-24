import redis
from config import settings

redis_client = redis.StrictRedis(host=settings.REDIS.get('host'), port=settings.REDIS.get('port'),
                                 db=settings.REDIS.get('db'))
