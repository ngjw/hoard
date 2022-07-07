import pytest
import threading
from math import inf
from hoard import FSHoard
from hoard import HashedFSHoard
from hoard import RedisHoard
from hoard import LRURedisHoard
from hoard import CachedHoard
from hoard import LRUCachedHoard
from hoard import DictHoard
from hoard import HoardSet
from hoard import CompositeHoard
from hoard import HoardView
from hoard import ReadOnlyHoard
from hoard.remote import RemoteHoardServer
from hoard.remote import RemoteHoard

def _test_hoard(h):

    assert not 'foo' in h
    x = (1, 2, 'three')
    h['foo'] = x

    x2 = h['foo']

    assert x == x2
    assert 'foo' in h

    y = (4, 2, 0)
    h['bar'] = y
    assert y == h['bar']
    assert 'bar' in h

    del h['bar']
    assert not 'bar' in h

    for i, x in enumerate('the quick brown fox'.split(' ')):
        h[x] = i

    list(h.keys())

def test_fshoard(tmpdir):

    def _test(hoard_cls):
        hoard = hoard_cls.new(tmpdir / 'hoard', remove_existing=True)
        _test_hoard(hoard)

        _test_hoard(hoard / 'partition')

        hoard = hoard_cls.new(tmpdir / 'hoard', remove_existing=True).redis_cached()
        hoard.cache.delete()
        _test_hoard(hoard)

        hoard = hoard_cls.new(tmpdir / 'hoard', remove_existing=True)

        keys = set()
        for i in range(5):
            k = f'dir{i}/item{i}'
            keys.add(k)
            hoard[k] = i

        assert set(hoard.keys()) == keys

    _test(HashedFSHoard)
    _test(FSHoard)

def test_redis_hoard():

    hoard = RedisHoard.new('hoard_test', remove_existing=True)
    _test_hoard(hoard)

    hoard = RedisHoard.new('hoard_test', remove_existing=True)
    _test_hoard(DictHoard.cache(hoard))

def test_redis_lru_hoard():

    hoard = LRURedisHoard.new('lru_hoard_test', maxsize=5)

    for k in map(str, range(10)):
        hoard[k] = k
        hoard[k]
        assert hoard.redis.zcount(hoard.zkey, -inf, inf) <= 5

    assert hoard.redis.zpopmin(hoard.zkey)[0][0].decode() == '5'

def test_cache(tmpdir):

    base = RedisHoard.new('hoard_test', remove_existing=True)

    _test_hoard(CachedHoard(base))

    base = FSHoard.new(tmpdir / 'hoard', remove_existing=True)
    _test_hoard(CachedHoard(base))

def test_lru_cache(tmpdir):

    base = FSHoard.new(tmpdir / 'hoard', remove_existing=True)
    _test_hoard(LRUCachedHoard(base))

    base = FSHoard.new(tmpdir / 'hoard2', remove_existing=True)
    h = LRUCachedHoard(base, 5)

    for k in map(str, range(10)):
        h[k] = k
        h[k]
        del base[k]

    for k in map(str, range(5)):
        assert not k in h
        with pytest.raises(KeyError):
            assert h[k]

    for k in map(str, range(5, 10)):
        assert not k in h
        h[k]

def test_remote_hoard():

    base = DictHoard()
    for i in range(5):
        base[i] = i * i

    TEST_PORT = 58585

    rhs = RemoteHoardServer({'foo': base}, '127.0.0.1', TEST_PORT)
    rh = RemoteHoard('foo', port=TEST_PORT)

    for k, v in base.items():
        assert rh[k] == v

    rh['foo'] = 'bar'
    assert base['foo'] == 'bar'

    rhs.stop()

def test_siphon():
    h1 = DictHoard()
    for i in range(10):
        h1[i] = i * i

    h2 = DictHoard()
    h2.siphon(h1)

    assert h1 == h2

def test_hoardset():

    h1, h2 = DictHoard(), DictHoard()

    hs = HoardSet({
        'h1': h1,
        'h2': h2,
    })

    hs['h1', 'foo1'] = 'foo1'
    hs['h2', 'foo2'] = 'foo2'

    assert ('h1', 'foo1') in hs
    assert ('h2', 'foo2') in hs

    assert h1['foo1'] == 'foo1'
    assert h2['foo2'] == 'foo2'

    del hs['h1', 'foo1']
    assert 'foo1' not in h1

def test_composite():

    h1, h2 = DictHoard(), DictHoard()

    h1['foo'] = 'h1'
    h2['foo'] = 'h2'
    h2['bar'] = 'h2'

    ch = CompositeHoard([h1, h2])

    assert ch['foo'] == 'h1'
    assert ch['bar'] == 'h2'

    assert len(list(ch.keys())) == 2

    with pytest.raises(RuntimeError):
        ch['foo'] = 10

    ch = CompositeHoard([h1, h2], write_idx=0)

    ch['x'] = 10
    del ch['foo']
    assert ch['foo'] == 'h2'

def test_readonly():
    h = DictHoard()
    h['1'] = 1
    h['2'] = 2
    h['3'] = 3

    ro = ReadOnlyHoard(h)
    assert ro['1'] == 1

    with pytest.raises(RuntimeError):
        ro['2'] = 2

    with pytest.raises(RuntimeError):
        del ro['2']

def test_view():
    main = DictHoard()
    view_map = DictHoard()

    for i in range(10):
        main[str(i)] = f'main{i}'

    view_map['0'] = '2'
    view_map['2'] = '0'

    hv = HoardView(main, lambda k: view_map.get(k, k))
    assert hv['0'] == 'main2'

def test_match_delete():

    keys = 'the quick brown fox jumps over the lazy dog'.split(' ')

    h = DictHoard()

    for k in keys:
        h[k] = k

    h.delete(*h.match('[jf].*'))

    assert not 'fox' in h
    assert not 'jumps' in h
