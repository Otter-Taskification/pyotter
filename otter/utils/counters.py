from itertools import count
from collections import defaultdict
from typing import Union, List, Callable, Iterable, Optional


class CountingDict(defaultdict):
    """A defaultdict which uses ``next(counter)`` to uniquely number its keys"""

    def __init__(self, counter: Optional[count] = None):
        counter = counter or count()
        super().__init__(lambda: next(counter))


class SequenceLabeller:
    """
    Determine labels for a sequence of elements. For elements where the given predicate is True, use the group_by
    argument to determine a common label to use. Where it is False assign a unique label.
    """

    def __init__(
        self, predicate: Callable, group_label: Union[Callable, str] = "event"
    ):
        if isinstance(group_label, str):
            self._group_label = lambda item: item[group_label]
        elif callable(group_label):
            self._group_label = group_label
        else:
            raise TypeError("group_by must be callable or str")
        self._predicate = predicate

    def label(self, sequence: Iterable) -> List[int]:
        is_true = list(map(self._predicate, sequence))
        count_items = len(sequence)
        assert count_items == len(is_true)
        count_true = sum(is_true)
        count_not_true = count_items - count_true
        vertex_counter = count()
        get_label = CountingDict(counter=count(start=count_not_true))
        return [
            get_label[self._group_label(item)] if item_true else next(vertex_counter)
            for item, item_true in zip(sequence, is_true)
        ]
