import io
from hoard.serialize import Pickler
from hoard.serialize import JSONer
from hoard.serialize import BinarySerializer
from hoard.serialize import TextSerializer

def test_pickler():


    def _test(serializer, x):
        f = io.BytesIO()
        serializer.to_stream(x, f)
        f.seek(0)
        assert x == serializer.from_stream(f)
        assert x == serializer.unserialize(serializer.serialize(x))

    _test(Pickler(), (1,2,3))
    _test(Pickler(), ['a','b','c'])

    _test(JSONer(), ['a','b','c'])

    _test(TextSerializer(), 'the quick brown fox')
    _test(BinarySerializer(), b'the quick brown fox')
