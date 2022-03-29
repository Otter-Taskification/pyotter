from collections import Counter
from itertools import chain, count
from collections import defaultdict
from typing import Union, List, Callable, Iterable

class PrettyCounter(Counter):

    def __repr__(self):
        return "\n".join([f"{self[k]:>6} {k}" for k in self]) + f"\nTotal count: {sum(self.values())}"

class VertexLabeller:

    def __init__(self, cond: Callable, group_key: Union[Callable, str] = "event"):
        if isinstance(group_key, str):
            self._group_func = lambda v: v[group_key]
        elif callable(group_key):
            self._group_func = group_key
        else:
            raise TypeError("key must be callable or str")
        self._cond = cond

    def label(self, vertices: Iterable):
        vertex_counter = count()
        cluster_counter = count(start=sum(not self._cond(x) for x in vertices))
        get_label = defaultdict(lambda: next(cluster_counter))
        return [get_label[self._group_func(x)] if self._cond(x) else next(vertex_counter) for x in vertices]
