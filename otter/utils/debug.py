import sys
import pdb
from contextlib import contextmanager


@contextmanager
def post_mortem(catch: bool):
    if catch:
        try:
            yield
        except Exception:
            exc_type, exc, tb = sys.exc_info()
            print(exc)
            print(f"caught {exc_type}, entering post-mortem")
            pdb.post_mortem(tb)
    else:
        yield
