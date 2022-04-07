from contextlib import contextmanager
from .utils import renamekwargs


class tag:

    def __init__(self, name, **kwargs):
        self._name = name
        self._kwargs = renamekwargs(kwargs)
        self._content = None

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


class Doc:

    def __init__(self, doctype: str="<!DOCTYPE html>", sep="\n"):
        self._content = [doctype]
        self._sep = sep

    def add(self, newtag, **kwargs):
        t = tag(newtag, **kwargs) if isinstance(newtag, str) else newtag
        self.extend(t)

    @contextmanager
    def open(self, tagname, **kwargs):
        with tag(tagname, **kwargs) as t:
            yield t
        self.extend(t)

    def extend(self, t: tag):
        self._content.extend(t)

    def append(self, t: tag):
        self._content.append(t)

    def __iter__(self):
        return iter(self._content)

    def __repr__(self):
        return self._sep.join(str(item) for item in self)
