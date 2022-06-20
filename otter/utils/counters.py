from collections import Counter
from itertools import chain, count
from collections import defaultdict
from typing import Union, List, Callable, Iterable


class PrettyCounter(Counter):

    def __repr__(self):
        return "\n".join([f"{self[k]:>6} {k}" for k in self]) + f"\nTotal count: {sum(self.values())}"


class label_groups_if:
    # Label elements in a sequence. For elements where a condition is True,
    # determine the label from a grouping rule. Otherwise assign a unique label.

    def __init__(self, cond: Callable, group_by: Union[Callable, str] = "event"):
        if isinstance(group_by, str):
            self._group_by = lambda v: v[group_by]
        elif callable(group_by):
            self._group_by = group_by
        else:
            raise TypeError("key must be callable or str")
        self._cond = cond

    def apply_to(self, vertices: Iterable):
        vertex_counter = count()
        cluster_counter = count(start=sum(not self._cond(x) for x in vertices))
        get_label = defaultdict(lambda: next(cluster_counter))
        return [get_label[self._group_by(x)] if self._cond(x) else next(vertex_counter) for x in vertices]
