from .hoard import Hoard
from functools import cached_property

class HoardSet(Hoard):

    """
    API into set of hoards
    KEYS:
      HOARDSET[HOARD_NAME, KEY] = HOARD[KEY]
    """

    def __init__(self, hoards):
        self.hoards = hoards

    def __getitem__(self, k):
        hoard, key = k
        return self.hoards[hoard][key]

    def __setitem__(self, k, v):
        hoard, key = k
        self.hoards[hoard][key] = v

    def __delitem__(self, k):
        hoard, key = k
        del self.hoards[hoard][key]

    def __contains__(self, k):
        hoard, key = k
        return hoard in self.hoards and key in self.hoards[hoard]

    def keys(self):
        for name, hoard in self.hoards.items():
            for k in hoard.keys():
                yield name, k

    def __repr__(self):
        return f'<{type(self).__name__} {list(self.hoards)}>'

class CompositeHoard(Hoard):

    """
    An ordered set of hoards appearing as one
    Lookup from left to right (i.e. keys in an earlier hoard overrides similar keys in later hoards)
    One hoard may be writeable, indicated by index in the list of hoards
    """

    def __init__(self, hoards, write_idx=None):
        self.hoards = tuple(hoards)
        self.write_idx = write_idx

    @cached_property
    def writeable(self):
        if self.write_idx is None:
            raise RuntimeError('No writeable hoard selected')
        return self.hoards[self.write_idx]

    def __getitem__(self, k):
        for h in self.hoards:
            try:
                return h[k]
            except KeyError:
                pass
        else:
            raise KeyError(key)

    def __setitem__(self, k, v):
        self.writeable[k] = v

    def __delitem__(self, k):
        del self.writeable[k]

    def contains(self, k):
        return any(k in h for h in self.hoards)

    def keys(self):
        seen = set()
        for h in self.hoards:
            for k in h.keys():
                if k in seen:
                    continue
                seen.add(k)
                yield k
