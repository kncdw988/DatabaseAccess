import json
import time
from datetime import datetime

import redis
from config import Config
from .utils import RedisDataSerializer


class RedisAccess:

    def __init__(self, db=0):
        self.pool = redis.ConnectionPool(
            host = Config.REDIS_HOST,
            port = Config.REDIS_PORT,
            db = db,
            username = Config.REDIS_USER,
            password = Config.REDIS_PWD,
            decode_responses=True,
            max_connections=10
        )
        self.redis = redis.Redis(connection_pool=self.pool)

class RedisBase:

    _prefix_key = Config.REDIS_PREFIX
    _db = 0

    def __init__(self, name):
        self.redis = RedisAccess(self._db).redis
        self.key = f"{self._prefix_key}{name}"

    def expire(self, timeout):
        return self.redis.expire(self.key, timeout)

    def expireat(self, when):
        return self.redis.expireat(self.key, when)

    def delete(self):
        return self.redis.delete(self.key)

    def exists(self):
        return self.redis.exists(self.key)

class RedisHash(RedisBase):

    _prefix_key = f"{RedisBase._prefix_key}hash:"
    _db = 0

    def hget(self, key):
        return self.redis.hget(self.key, key)

    def hmget(self, *keys):
        return self.redis.hmget(self.key, *keys)

    def hgetall(self):
        return self.redis.hgetall(self.key)

    def hkeys(self):
        return self.redis.hkeys(self.key)

    def hexists(self, key):
        return self.redis.hexists(self.key, key)

    def hdel(self, *keys):
        return self.redis.hdel(self.key, *keys)

    def hset(self, key, value):
        return self.redis.hset(self.key, key, value)

    def hmset(self, mapping):
        return self.redis.hmset(self.key, mapping)

class RedisStream(RedisBase):

    _prefix_key = f"{RedisBase._prefix_key}stream:"
    _db = 1
    

    def xadd(self, fields, stream_id, sequenceNumber):
        return self.redis.xadd(self.key, fields, f"{stream_id}-{sequenceNumber}", maxlen=500000)

    def xread(self, stream_id=0, count=None):
        return self.redis.xread({self.key: stream_id}, count=count)

    def xread_block(self, block=None):
        return self.redis.xread({self.key: "$"}, block=block)

    def stream_info(self):
        return self.redis.xinfo_stream(self.key)

    def xlen(self):
        return self.redis.xlen(self.key)

class RedisStreamQueue(RedisStream):

    _prefix_key = f"{RedisBase._prefix_key}stream_queue:"
    _xgroup_name = "default_group"
    _consumer_name = "default_consumer"

    def _create_group(self):
        group_infos = self.group_info()
        is_exist = False
        for i in group_infos:
            if self._xgroup_name == i.get("name"):
                is_exist = True
                break
        if not is_exist:
            self.create_group()

    def xread_group(self, count=1, block=None, noack=False):
        return self.redis.xreadgroup(
            self._xgroup_name, self._consumer_name,
            {self.key: ">"}, count=count, block=block, noack=noack
        )

    def xack(self, *stream_ids):
        return self.redis.xack(self.key, self._xgroup_name, *stream_ids)
    
    def create_group(self):
        return self.redis.xgroup_create(self.key, self._xgroup_name)

    def group_info(self):
        return self.redis.xinfo_groups(self.key)

    def send(self, message):
        timestamp = time.time()
        print(timestamp)
        return self.xadd(RedisDataSerializer.dumps(message), int(timestamp * 10000), 0)

    def watch(self, block=1000*2): 
        self._create_group()
        res = self.xread_group(block, noack=True)
        if res:
            return res[0][1]
        return []
