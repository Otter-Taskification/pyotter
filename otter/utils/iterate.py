from __future__ import annotations

import itertools as it
from collections.abc import Iterable
from typing import TypeVar

T = TypeVar("T")


def batched(iterable: Iterable[T], batch_size: int) -> Iterable[Iterable[T]]:
    # credit: https://docs.python.org/3/library/itertools.html#itertools-recipes
    if batch_size < 1:
        raise ValueError("batch_size must be at least one")
    items = iter(iterable)
    while True:
        batch = tuple(it.islice(items, batch_size))
        if batch:
            yield batch
        else:
            break
