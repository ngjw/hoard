import io
import rsa
from .hoard import Hoard

class BaseSecretHoard(Hoard):

    """
    Encrypt data
    """

    def __init__(self, base, serializer='pickle'):
        self.base = base
        self.serializer_type = serializer

    def keys(self):
        yield from self.base.keys()

    def __delitem__(self, k):
        del self.base[k]

    def __contains__(self, k):
        return k in self.base

    def load_raw(self, k):
        return self.decrypt(self.base.load_raw(k))

    def store_raw(self, k, v):
        self.base.store_raw(k, self.encrypt(v))

    def encrypt(self, stream):
        raise NotImplementedError

    def decrypt(self, stream):
        raise NotImplementedError


class SecretHoard(BaseSecretHoard):

    """
    RSA Hoard
    """

    def __init__(self, base, pubkey, privkey=None, serializer='pickle'):
        BaseSecretHoard.__init__(self, base, serializer=serializer)
        self.privkey = privkey
        self.pubkey = pubkey

    def encrypt(self, stream):
        b = io.BytesIO()
        b.write(rsa.encrypt(stream.read(), self.pubkey))
        b.seek(0)
        return b

    def decrypt(self, stream):
        b = io.BytesIO()
        if self.privkey is None:
            raise Exception(f'No private key. Write only')
        b.write(rsa.decrypt(stream.read(), self.privkey))
        b.seek(0)
        return b
