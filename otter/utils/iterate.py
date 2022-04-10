import itertools as it
from collections import defaultdict
from collections.abc import Iterable

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
def flatten(args, exclude=(str, bytes, tuple)):
    assert isinstance(exclude, tuple)
    for item in args:
        if isinstance(item, Iterable) and not isinstance(item, exclude):
            yield from flatten(item, exclude=exclude)
        else:
            yield item

def transpose_list_to_dict(list_of_dicts):
    D = defaultdict(list)
    all_keys = set(flatten(d.keys() for d in list_of_dicts))
    for d in list_of_dicts:
        for key in all_keys:
            D[key].append(d.get(key, None))
    return D
