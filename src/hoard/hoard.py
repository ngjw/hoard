import re
import shutil
import tempfile
import pathlib
import contextlib
from itertools import chain
from functools import cached_property

from .serialize import Serializer


class Hoard:

    def __delitem__(self, k):
        raise NotImplementedError

    def __contains__(self, k):
        raise NotImplementedError

    def keys(self):
        raise NotImplementedError

    def match(self, pattern):
        if isinstance(pattern, str):
            pattern = re.compile(pattern)

        yield from filter(pattern.match, self.keys())

    def delete(self, *keys):
        for k in keys:
            del self[k]

    def load_raw(self, k):
        raise NotImplementedError

    def store_raw(self, k, stream):
        raise NotImplementedError

    @cached_property
    def serializer(self):
        try:
            serializer_type = self.serializer_type
        except AttributeError:
            serializer_type = 'pickle'
        return Serializer.get(serializer_type)()

    def __setitem__(self, k, v):
        self.store_raw(k, self.serializer.as_stream(v))

    def __getitem__(self, k):
        return self.serializer.from_stream(self.load_raw(k))

    def update(self, d={}, **kwargs):
        for k, v in chain(d.items(), kwargs.items()):
            self[k] = v

    def __iter__(self):
        yield from self.keys()

    def items(self):
        for k in self.keys():
            yield k, self[k]

    def values(self):
        for k in self.keys():
            yield self[k]

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    @contextlib.contextmanager
    def open(self, k, mode='r', *args, **kwargs):

        with self.as_file(k) as fn:
            pathlib.Path(fn).parent.mkdir(exist_ok=True, parents=True)
            with open(fn, mode, *args, **kwargs) as f:
                yield f


    @contextlib.contextmanager
    def as_file(self, k, wd=None):

        wd = pathlib.Path(wd or tempfile.mkdtemp())

        fn = wd / 'hoardfile'

        if k in self:
            shutil.copyfileobj(self.load_raw(k), open(fn, 'wb'))

        try:
            yield fn

        finally:
            self.store_raw(k, open(fn, 'rb'))

    def siphon(self, source, overwrite=False):
        for k in source:
            if overwrite or not k in self:
                self[k] = source[k]

    def sync(self, other):
        """
        A -> union(A, B - A)
        B -> union(B, A - B)
        """
        self.siphon(other)
        other.siphon(self)


class ReadOnlyHoard(Hoard):

    def __init__(self, base):
        self.base = base

    def __setitem__(self, k, v):
        raise RuntimeError('Hoard is read-only')

    def __getitem__(self, k):
        return self.base[k]

    def __delitem__(self, k):
        raise RuntimeError('Hoard is read-only')

    def __contains__(self, k):
        return k in self.base

    def keys(self):
        return self.base.keys()
