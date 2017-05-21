import collections


class LazyDict(collections.MutableMapping):

    def __init__(self):
        self._inner = None
        self._dirty = False

    @property
    def dirty(self):
        return self._dirty

    @property
    def inner(self):
        return self._inner

    def _evaluate(self):
        raise NotImplementedError()

    def __ensure(self):
        if self._inner is None:
            self._inner = self._evaluate()

    def __len__(self):
        self.__ensure()
        return len(self._inner)

    def __getitem__(self, item):
        self.__ensure()
        return self._inner.__getitem__(item)

    def __setitem__(self, key, value):
        self.__ensure()
        self._inner.__setitem__(key, value)
        self._dirty = True

    def __delitem__(self, key):
        self.__ensure()
        self._inner.__delitem__(key)
        self._dirty = True

    def __iter__(self):
        self.__ensure()
        return self._inner.__iter__()
