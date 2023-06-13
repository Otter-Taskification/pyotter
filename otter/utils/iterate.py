import itertools as it
from collections import defaultdict
from collections.abc import Iterable
from typing import TypeVar

T = TypeVar("T")

# TODO: seems to be dead code, not used anywhere
def pairwise(iterable):
    # https://docs.python.org/3/library/itertools.html#itertools.pairwise

    # 2 copies of iterable
    a, b = it.tee(iterable)

    # wind b on by one (so a is longer by one)
    # next(b, None)
    # return it.zip_longest(a, b)

    b = it.chain([None], b)
    return zip(b,a)

# credit: https://stackoverflow.com/a/2158532
def flatten(args, exclude: list=None):
    no_flatten: list = [str, bytes, tuple]
    if exclude is not None:
        no_flatten.extend(exclude)
    for item in args:
        if isinstance(item, Iterable):
            if isinstance(item, tuple(no_flatten)):
                yield item
            else:
                yield from flatten(item, exclude=exclude)
        else:
            yield item

def transpose_list_to_dict(list_of_dicts, allow_missing: bool = True):
    D = defaultdict(list)
    all_keys = set(flatten(d.keys() for d in list_of_dicts))
    for d in list_of_dicts:
        for key in all_keys:
            value = d.get(key, None)
            if value is not None or allow_missing:
                D[key].append(value)
    return D

def batched(iterable: Iterable[T], n) -> Iterable[Iterable[T]]:
    # credit: https://docs.python.org/3/library/itertools.html#itertools-recipes
    if n < 1:
        raise ValueError('n must be at least one')
    items = iter(iterable)
    while True:
        batch = tuple(it.islice(items, n))
        if batch:
            yield batch
        else:
            break
