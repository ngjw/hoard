import io
import time
import json
import pickle as pk
from math import inf
from redis import Redis
from functools import cache, cached_property

from .hoard import Hoard
from .cache import Cache
from .serialize import Serializer


class RedisHoard(Hoard):

    def __init__(self, redis_key, redis_kwargs={}):
        self.redis_key = redis_key.encode()
        self.redis = Redis(**redis_kwargs)

    @cached_property
    def config_key(self):
        return (f'__HOARDCONFIG.{self.redis_key}').encode()

    @cache
    def get_config(self, k, default=None):
        raw = self.redis.hget(self.config_key, k)
        if not raw:
            return default
        return json.loads(self.redis.hget(self.config_key, k).decode())

    def set_config(self, k, v):
        return self.redis.hset(self.config_key, k, json.dumps(v))

    @classmethod
    def new(cls, redis_key, redis_kwargs={}, remove_existing=False, serializer='pickle'):
        h = cls(redis_key, redis_kwargs)
        if remove_existing:
            h.delete()
        else:
            if h.redis.keys(redis_key):
                raise ValueError(f'Key {redis_key} already exists')
        h.set_config('serializer', serializer)
        return h

    def delete(self):
        self.redis.delete(self.redis_key)
        self.redis.delete(self.config_key)

    def keys(self):
        for k in self.redis.hkeys(self.redis_key):
            yield k.decode()

    def load_raw(self, k):
        return io.BytesIO(self.redis.hget(self.redis_key, k.encode()))

    def store_raw(self, k, stream):
        return self.redis.hset(self.redis_key, k.encode(), stream.read())

    def __delitem__(self, k):
        self.redis.hdel(self.redis_key, k.encode())

    def __contains__(self, k):
        return self.redis.hexists(self.redis_key, k.encode())

    @cached_property
    def serializer(self):
        return Serializer.get(self.get_config('seralizer', 'pickle'))()


class LRURedisHoard(RedisHoard, Cache):

    def __init__(self, redis_key, redis_kwargs={}):
        RedisHoard.__init__(self, redis_key, redis_kwargs)

    @cached_property
    def zkey(self):
        return (f'__LRU_z.{self.redis_key}').encode()

    @classmethod
    def new(cls, redis_key, maxsize, redis_kwargs={}, remove_existing=False, serializer='pickle'):
        h = super(LRURedisHoard, cls).new(redis_key, redis_kwargs, remove_existing)
        if remove_existing:
            h.redis.delete(h.zkey)
            h.redis.delete(h.config_key)
        else:
            if h.redis.keys(h.zkey):
                raise ValueError(f'Key {h.zkey} already exists')
        h.set_config('maxsize', maxsize)
        h.set_config('serializer', serializer)
        return h

    @cached_property
    def maxsize(self):
        return self.get_config('maxsize')

    def stamp(self, k):
        now = time.time()
        self.redis.zadd(self.zkey, {k: time.time()})

    def __setitem__(self, k, v):
        RedisHoard.__setitem__(self, k, v)
        now = time.time()
        self.stamp(k)
        self.prune()

    def __getitem__(self, k):
        try:
            v = RedisHoard.__getitem__(self, k)
        except KeyError:
            raise KeyError
        self.stamp(k)
        self.prune()
        return v

    def __delitem__(self, k):
        RedisHoard.__delitem__(self, k)
        self.redis.zrem(k)

    def prune(self):
        n = self.redis.zcount(self.zkey, -inf, inf)
        if n > self.maxsize:
            self.redis.zpopmin(self.zkey, n - self.maxsize)
