"""
Key functionality:

- load OTF2 trace and extract locations, events, regions, attributes so they can be easily looked up
- parse loaded trace to determine what graph nodes to create and where edges are required
- export execution graph to file
"""

from .EventFactory import EventFactory, events
from .TaskRegistry import TaskRegistry, tasks
from .args import get_args
from .definitions import TaskStatus
from .ChunkFactory import ChunkFactory, chunks
from .logging import get_logger
from .utils import decorate

def _check_dot():
    # Check that the "dot" commandline utility is available
    from shutil import which
    if which("dot") is None:
        print(f"Error: {__name__} couldn't find the graphviz command line utility \"dot\" (see https://graphviz.org/download/).")
        print("Please install graphviz before continuing.")
        quit()

_check_dot()
