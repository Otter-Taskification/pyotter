"""
Key functionality:

- load OTF2 trace and extract locations, events, regions, attributes so they can be easily looked up
- parse loaded trace to determine what graph nodes to create and where edges are required
- export execution graph to file
"""

from .args import get_args
args = get_args()

from . import log
log.Logging.init(args)

from .EventFactory import EventFactory, events
from .TaskRegistry import TaskRegistry, tasks
from .definitions import TaskStatus
from .ChunkFactory import ChunkFactory, chunks

def _check_dot():
    # Check that the "dot" commandline utility is available
    from shutil import which
    if which("dot") is None:
        print(f"Error: {__name__} couldn't find the graphviz command line utility \"dot\" (see https://graphviz.org/download/).")
        print("Please install graphviz before continuing.")
        quit()

_check_dot()
