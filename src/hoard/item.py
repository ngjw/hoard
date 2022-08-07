from functools import wraps
from dataclasses import dataclass

@dataclass
class HoardItem:

    HOARD = None

    @property
    def key(self):
        raise NotImplementedError

    def get(self, getter=None, store=False):
        try:
            return self.HOARD[self.key]
        except KeyError:
            if not getter:
                raise
            value = getter(self.key)
            if store:
                self.set(value)
            return value

    def delete(self):
        del self.HOARD[self.key]

    def set(self, value):
        self.HOARD[self.key] = value

    def exists(self):
        return self.key in self.HOARD

    @classmethod
    def cache(cls, func):

        @wraps(func)
        def cached_func(*args, **kwargs):
            item = cls(*args, **kwargs)
            return item.get(getter=lambda x: func(*args, **kwargs), store=True)

        return cached_func
