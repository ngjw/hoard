import os
import uuid
import gzip
import yaml
import shutil
import logging
import contextlib
import base58
from hashlib import sha1
from pathlib import Path
from functools import cached_property

from .hoard import Hoard
from .cache import CachedHoard
from .serialize import Serializer


class BaseFSHoard(Hoard):

    def __init__(self, path, partition=None):
        self.root = Path(path)
        self.partition = partition
        if not self.root.exists():
            raise RuntimeError(f'Path not found: {self.root}')

    @staticmethod
    def atomic_write(path, mode, open_func=open):
        path = Path(path)
        tmp = path.parent / f'.{path.name}.{uuid.uuid4()}.tmp'
        def _write(writer):
            writer(open_func(tmp, mode))
            os.rename(tmp, path)
        return _write

    @cached_property
    def data_root(self):
        suffix = '' if self.partition is None else f'.{self.partition}'
        return self.root / f'data{suffix}'

    @cached_property
    def compression(self):
        return self.config.get('compression', None)

    @cached_property
    def open_func(self):
        if self.compression is None:
            return open
        elif self.compression == 'gzip':
            return gzip.open
        else:
            raise ValueError(f'Unknown hoard compression {self.compression}')

    @property
    def config_path(self):
        return self.root / 'config.yaml'

    @cached_property
    def config(self):
        return yaml.load(open(self.config_path, 'r'), Loader=yaml.Loader)

    def __repr__(self):
        return f'<{type(self).__name__} @ {self.root}>'

    @cached_property
    def serializer(self):
        return Serializer.get(self.config.get('serializer', 'pickle'))()

    def store_raw(self, k , stream):
        p = self.get_path(k)
        p.parent.mkdir(parents=True, exist_ok=True)
        self.atomic_write(p, 'wb', open_func=self.open_func)(lambda fh: shutil.copyfileobj(stream, fh))

    def load_raw(self, k):
        p = self.get_path(k)
        try:
            return self.open_func(p, 'rb')
        except FileNotFoundError:
            raise KeyError(k)

    def __delitem__(self, k):
        p = self.get_path(k)
        os.remove(p)

    def __contains__(self, k):
        p = self.get_path(k)
        return p.exists()

    @contextlib.contextmanager
    def as_file(self, k, wd=None):
        try:
            yield self.get_path(k)
        finally:
            pass

    def __truediv__(self, partition):
        return type(self)(path=self.root, partition=partition)

    @classmethod
    def encode_key(cls, key):
        return base58.b58encode(key.encode()).decode()

    @classmethod
    def decode_key(cls, key):
        return base58.b58decode(key.encode()).decode()


class FSHoard(BaseFSHoard):

    @classmethod
    def new(cls, path, compression=None, remove_existing=False, serializer='pickle'):

        p = Path(path)

        if p.exists():
            logging.warning(f'Hoard path exists: {p}')
            if not remove_existing:
                raise FileExistsError(p)
            else:
                logging.warning(f'Removing {p}')
                shutil.rmtree(p)

        p.mkdir(parents=True)
        h = cls(path)
        h.data_root.mkdir(parents=True, exist_ok=True)

        config = {'compression': compression, 'serializer': serializer}

        cls.atomic_write(h.config_path, 'w')(lambda fh: fh.write(yaml.dump(config)))
        return h

    def get_path(self, key):
        fn = self.encode_key(key)
        return Path(self.data_root / fn)

    def keys(self):
        for f in Path(self.data_root).iterdir():
            yield self.decode_key(f.name)


class HashedFSHoard(BaseFSHoard):

    @classmethod
    def new(cls, path, depth=3, compression=None, remove_existing=False, serializer='pickle'):

        p = Path(path)

        if p.exists():
            logging.warning(f'Hoard path exists: {p}')
            if not remove_existing:
                raise FileExistsError(p)
            else:
                logging.warning(f'Removing {p}')
                shutil.rmtree(p)

        p.mkdir(parents=True)
        h = cls(path)
        h.data_root.mkdir(parents=True, exist_ok=True)

        config = {'compression': compression, 'serializer': serializer, 'depth': depth}

        cls.atomic_write(h.config_path, 'w')(lambda fh: fh.write(yaml.dump(config)))
        return h

    @cached_property
    def depth(self):
        return self.config['depth']

    @staticmethod
    def hash(x):
        h = sha1(str(x).encode())
        return int.from_bytes(h.digest(), byteorder='big')

    def get_path(self, key):
        k = self.encode_key(key)
        q = self.hash(k)
        p = self.data_root
        for i in range(self.depth):
            q, r = divmod(q, 100)
            p = p / str(r)
        return p / k

    def keys(self):
        for root, dirs, files in os.walk(self.data_root):
            for f in files:
                rp = os.path.relpath(os.path.join(root, f), self.data_root)
                key = rp.split(os.path.sep, self.depth)[-1]
                yield self.decode_key(key)
