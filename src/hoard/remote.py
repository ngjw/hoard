import logging
import threading
import pickle as pk
from functools import cached_property
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy

from .hoard import Hoard

DEFAULT_PORT = 52000

logger = logging.getLogger(__name__)

class RemoteHoardServer:

    def __init__(self, hoards, host='0.0.0.0', port=DEFAULT_PORT):
        self.hoards = hoards
        self.server = SimpleXMLRPCServer((host, port))
        self.register_functions()
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()

    def stop(self):
        logger.warning('shutting down RemoteHoardServer')
        self.server.shutdown()
        self.thread.join()
        self.thread = None

    def register_functions(self):
        self.register_binary(self._getitem)
        self.register_binary(self._setitem)
        self.register_binary(self._delitem)
        self.register_binary(self._contains)
        self.register_binary(self._keys)
        self.server.register_function(self._check, '_check')

    def register_binary(self, func):
        self.server.register_function(lambda *a, **k: pk.dumps(func(*a, **k)), func.__name__)

    def _getitem(self, h, k):
        return self.hoards[h][k]

    def _setitem(self, h, k, v):
        logging.info(f'Setting {k} on {h}')
        self.hoards[h][k] = v

    def _delitem(self, h, k):
        logging.info(f'Deleting {k} on {h}')
        del self.hoards[h][k]

    def _contains(self, h, k):
        return k in self.hoards[h]

    def _keys(self, h):
        return list(self.hoards[h].keys())

    def _check(self, h):
        return h in self.hoards

class RemoteHoard(Hoard):

    def __init__(self, hoard, host='localhost', port=DEFAULT_PORT):
        self.hoard = hoard
        self.host = host
        self.port = port
        self.proxy = ServerProxy(self.url)
        if not self.proxy._check(hoard):
            raise ValueError(f'{hoard} not found on remote server')

    @cached_property
    def url(self):
        return f'http://{self.host}:{self.port}'

    def __call__(self, func):
        def _f(*args, **kwargs):
            b = getattr(self.proxy, func)(self.hoard, *args, **kwargs)
            return pk.loads(b.data)
        return _f

    def __setitem__(self, k , v):
        return self('_setitem')(k, v)

    def __getitem__(self, k):
        return self('_getitem')(k)

    def __delitem__(self, k):
        return self('_delitem')(k)

    def __contains__(self, k):
        return self('_contains')(k)

    def keys(self):
        yield from self('_keys')()

    def __getstate__(self):
        return {
            'hoard': self.hoard,
            'host': self.host,
            'port': self.port,
        }

    def __setstate__(self, state):
        self.hoard = state['hoard']
        self.host = state['host']
        self.port = state['port']
        self.proxy = ServerProxy(self.url)
