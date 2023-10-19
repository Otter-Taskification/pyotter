from collections import defaultdict
from itertools import count
from typing import Optional


class LabellingDict(defaultdict):
    """A defaultdict which uses ``next(counter)`` to uniquely number its keys"""

    def __init__(self, counter: Optional[count] = None):
        counter = counter or count()
        super().__init__(lambda: next(counter))


class CountingDict(defaultdict):
    """A dict which maintains a unique counter for each key"""

    def __init__(self, start: int = 0, step: int = 1):
        super().__init__(int)
        self._map = defaultdict(lambda: count(start=start, step=step))

    def increment(self, key):
        self[key] = next(self._map[key])
        return self[key]
