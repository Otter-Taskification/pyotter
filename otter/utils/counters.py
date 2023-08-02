from collections import defaultdict
from itertools import count
from typing import Optional


class CountingDict(defaultdict):
    """A defaultdict which uses ``next(counter)`` to uniquely number its keys"""

    def __init__(self, counter: Optional[count] = None):
        counter = counter or count()
        super().__init__(lambda: next(counter))
