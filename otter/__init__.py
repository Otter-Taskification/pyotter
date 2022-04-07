from . import log
from .args import get_args
from .EventFactory import EventFactory, events, unpack
from .TaskRegistry import TaskRegistry, tasks
from .definitions import TaskStatus
from .ChunkFactory import ChunkFactory, chunks
from .report import write_report


def _check_dot():
    # Check that the "dot" commandline utility is available
    from shutil import which
    if which("dot") is None:
        print(f"Error: {__name__} couldn't find the graphviz command line utility \"dot\" (see https://graphviz.org/download/).")
        print("Please install graphviz before continuing.")
        quit()


def interact(locals, g):
    import os
    import code
    import atexit
    import readline

    readline.parse_and_bind("tab: complete")

    hfile = os.path.join(os.path.expanduser("~"), ".otter_history")

    try:
        readline.read_history_file(hfile)
        numlines = readline.get_current_history_length()
    except FileNotFoundError:
        open(hfile, 'wb').close()
        numlines = 0

    atexit.register(append_history, numlines, hfile)

    k = ""
    for k, v in locals.items():
        if g is v:
            break

    banner = f"""
Graph {k} has {g.vcount()} nodes and {g.ecount()} edges

Entering interactive mode...
    """

    console = code.InteractiveConsole(locals=locals)
    console.interact(banner=banner, exitmsg=f"history saved to {hfile}")


def append_history(lines, file):
    newlines = readline.get_current_history_length()
    readline.set_history_length(1000)
    readline.append_history_file(newlines - lines, file)
