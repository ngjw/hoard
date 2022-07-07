from .hoard import Hoard

class HoardView(Hoard):

    def __init__(self, hoard, remap):
        self.hoard = hoard
        self.remap = remap

    def __getitem__(self, k):
        return self.hoard[self.remap(k)]

    def __setitem__(self, k, v):
        self.hoard[self.remap(k)] = v

    def __delitem__(self, k):
        del self.hoard[self.remap(k)]

    def __contains__(self, k):
        self.remap(k) in self.hoard
