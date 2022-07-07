from hoard import FSHoard
from hoard import DictHoard

def test_file(tmpdir):

    def _test(h):

        h['hash'] = 'foo'

        with h.as_file('hash') as fn:
            assert open(fn).read() == 'foo'

        with h.open('direct', 'w') as f:
            f.write('bar')

        assert h['direct'] == 'bar'

    hfile = FSHoard.new(tmpdir, remove_existing=True, serializer='text')

    hdict = DictHoard()
    hdict.serializer = hfile.serializer

    _test(hfile)
    _test(hdict)
