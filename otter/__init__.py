from importlib import metadata  # since 3.8

__version__ = metadata.version("otter")


def _find_dot_or_die():
    """Check that the "dot" commandline utility is available"""

    import shutil

    if shutil.which("dot") is None:
        print("Error: couldn't find the graphviz command line utility 'dot'.")
        print("Please install graphviz before continuing.")
        print("(see https://graphviz.org/download/)")
        raise SystemExit(0)


_find_dot_or_die()

from .definitions import TaskAttributes
from . import args, core, log, main, profile, project, reporting, utils, db
