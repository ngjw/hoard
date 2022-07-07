import io
import json
import pickle as pk

class Serializer:

    SERIALIZERS = {}

    @classmethod
    def register(cls, name):
        def decorator(subclass):
            cls.SERIALIZERS[name] = subclass
            return subclass
        return decorator

    def serialize(self, v):
        b = io.BytesIO()
        self.to_stream(v, b)
        return b.getvalue()

    def unserialize(self, v):
        return self.from_stream(io.BytesIO(v))

    def to_stream(self, v, fh):
        raise NotImplementedError

    def from_stream(self, fh):
        raise NotImplementedError

    @classmethod
    def get(cls, name):
        return cls.SERIALIZERS[name]

    def as_stream(self, v):
        b = io.BytesIO()
        self.to_stream(v, b)
        b.seek(0)
        return b


@Serializer.register('bytes')
class BinarySerializer(Serializer):

    def to_stream(self, v, fh):
        if not isinstance(v, bytes):
            raise ValueError(f'Expected bytes, got {type(v)}')
        fh.write(v)

    def from_stream(self, fh):
        return fh.read()


@Serializer.register('text')
class TextSerializer(Serializer):

    def to_stream(self, v, fh):
        if not isinstance(v, str):
            raise ValueError(f'Cannot encode unknown type: {type(v)}')
        fh.write(v.encode())

    def from_stream(self, fh):
        return fh.read().decode()


@Serializer.register('pickle')
class Pickler(Serializer):

    def to_stream(self, v, fh):
        pk.dump(v, fh, protocol=pk.HIGHEST_PROTOCOL)

    def from_stream(self, fh):
        return pk.load(fh)


@Serializer.register('json')
class JSONer(Serializer):

    def to_stream(self, v, fh):
        fh.write(json.dumps(v).encode()) # TODO: stream for big files

    def from_stream(self, fh):
        return json.load(fh)
