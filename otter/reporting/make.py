from __future__ import annotations

from collections import defaultdict
from contextlib import contextmanager

from .utils import renamekwargs


class Tag:
    def __init__(self, name, **kwargs):
        self._name = name
        self._kwargs = renamekwargs(kwargs)
        self._content: list[str] = []

    def __enter__(self):
        args = list()
        for key, value in self._kwargs.items():
            if isinstance(value, str):
                value = f'"{value}"'
            args.append(f"{key}={value}")
        args = " ".join(args)
        self._content = [f"<{self._name}", f" {args}" if args else "", ">"]
        self._content = ["".join(self._content)]
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._content.append(f"</{self._name}>")
        return self

    def __repr__(self):
        return "".join(str(item) for item in self._content)

    def __str__(self):
        return self.__repr__()

    def __iter__(self):
        return iter(self._content)

    def append(self, content):
        self._content.append(content)

    def extend(self, content):
        self._content.extend(content)

    @contextmanager
    def add(self, t):
        with t:
            yield t
        self.extend(t)


def wrap(name, content, **kwargs):
    with Tag(name, **kwargs) as tag:
        if content is not None:
            tag.append(content)
    return tag


def graphviz_record_table(object_attr: dict, **kwargs):
    """A HTML-like table where each row represents one key-value pair of some object's attributes"""
    table_attr = defaultdict(dict, kwargs.get("table_attr", dict()))
    with Tag("table", **table_attr["table"]) as t:
        for key, value in object_attr.items():
            with t.add(Tag("tr", **table_attr["tr"])) as tr:
                tr.append(wrap("td", wrap("b", key), **table_attr["td"]))
                tr.append(wrap("td", value, **table_attr["td"]))
    return t
