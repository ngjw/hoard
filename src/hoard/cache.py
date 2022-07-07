import io
from functools import cached_property, lru_cache
from .hoard import Hoard


class CachedHoard(Hoard):

    """
    Cache a hoard with another
    """

    def __init__(self, base, cache=None):
        self.base = base
        self.cache = DictHoard() if cache is None else cache

    def keys(self):
        yield from self.base.keys()

    def __setitem__(self, k, v):
        self.cache[k] = v
        self.base[k] = v

    def __getitem__(self, k):
        try:
            return self.cache[k]
        except KeyError:
            v = self.cache[k] = self.base[k]
            return v

    def __delitem__(self, k):
        del self.cache[k]
        del self.base[k]

    def __contains__(self, k):
        return (k in self.cache) or (k in self.base)

    def sync(self):
        for k in self.cache:
            if not k in base:
                del self.cache[k]


class LRUCachedHoard(Hoard):

    def __init__(self, base, maxsize=128):
        self.base = base
        self.cached_getter = lru_cache(maxsize=maxsize)(self.base.__getitem__)

    def __getitem__(self, k):
        return self.cached_getter(k)

    def __contains__(self, k):
        return k in self.base

    def __setitem__(self, k, v):
        self.base[k] = v

    def __delitem__(self, k):
        del self.base[k]

    def keys(self):
        yield from self.base.keys()


class Cache:

    @classmethod
    def cache(cls, base, *args, **kwargs):
        """
        Use this class of hoard to cache another hoard
        """
        return CachedHoard(base, cls(*args, **kwargs))


class DictHoard(dict, Hoard, Cache):

    def __repr__(self):
        return f'<{type(self).__name__} {dict.__repr__(self)}>'

    # Hoard.__getitem__ / Hoard.__setitem__ overridden by dict's methods
    def load_raw(self, k):
        b = self.serializer.serialize(self[k])
        return io.BytesIO(b)

    def store_raw(self, k, stream):
        self[k] = self.serializer.unserialize(stream.read())
