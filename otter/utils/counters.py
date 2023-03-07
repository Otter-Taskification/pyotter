from collections import Counter
from itertools import chain, count
from collections import defaultdict
from typing import Union, List, Callable, Iterable, TypeVar, Dict


class PrettyCounter(Counter):

    def __repr__(self):
        return "\n".join([f"{self[k]:>6} {k}" for k in self]) + f"\nTotal count: {sum(self.values())}"

class SequenceLabeller:
    """
    Determine labels for a sequence of elements. For elements where the given predicate is True, use the group_by
    argument to determine a common label to use. Where it is False assign a unique label.
    """

    # TODO: maybe the name "group_label" is more informative than "group_by"
    def __init__(self, predicate: Callable, group_by: Union[Callable, str] = "event"):
        if isinstance(group_by, str):
            self._group_by = lambda item: item[group_by]
        elif callable(group_by):
            self._group_by = group_by
        else:
            raise TypeError("group_by must be callable or str")
        self._predicate = predicate

    def label(self, sequence: Iterable) -> List[int]:
        is_true = list(map(self._predicate, sequence))
        count_not_true = len(is_true) - sum(is_true)
        vertex_counter = count()
        cluster_counter = count(start=count_not_true)
        get_label = defaultdict(lambda: next(cluster_counter))
        return [get_label[self._group_by(item)] if item_true else next(vertex_counter) for item, item_true in zip(sequence, is_true)]
