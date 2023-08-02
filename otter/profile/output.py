from contextlib import contextmanager
from cProfile import Profile


@contextmanager
def output(filename: str):
    """Write profiling data to filename if it is not None"""

    if filename:
        profile = Profile()
        profile.enable()
        yield
        profile.disable()
        profile.dump_stats(filename)
    else:
        yield
