from warnings import warn
from typing import Callable
from inspect import isgeneratorfunction, isfunction
from functools import partial
from .counters import SequenceLabeller
from .iterate import pairwise, flatten, transpose_list_to_dict
# from .vertex_predicates import key_is_not_none, \
#     is_region_type, \
#     is_empty_task_region, \
#     is_terminal_task_vertex, \
#     is_task_group_end_vertex, \
#     is_single_executor, \
#     is_master, \
#     is_taskwait
from . import vertex_predicates as vpred
# from .edge_predicates import edge_connects_same_type
from . import edge_predicates as epred
from .vertex_attr_handlers import LoggingValidatingReduction, ReductionDict
from . import vertex_attr_handlers as handlers
from ..log import get_logger
from .args import get_args
from .decorators import warn_deprecated


def dump_to_log_file(chunks, graphs, tasks):
    chunk_log = get_logger("chunks_debug")
    graph_log = get_logger("graphs_debug")
    task_log = get_logger("tasks_debug")

    graph_log.debug(">>> BEGIN GRAPHS <<<")
    chunk_log.debug(f">>> BEGIN CHUNKS <<<")

    for chunk, graph in zip(chunks, graphs):

        # write chunk
        for line in chunk.to_text():
            chunk_log.debug(f"{line}")

        # write graphs
        graph_log.debug(f"Chunk type: {chunk.type}")
        lines = [" ".join(f"{graph}".split("\n"))]
        for line in lines:
            graph_log.debug(f"{line}")
        for vertex in graph.vs:
            graph_log.debug(f"{vertex}")
        graph_log.debug("")

    chunk_log.debug(f">>> END CHUNKS <<<")
    graph_log.debug(">>> END GRAPHS <<<")

    task_log.debug(">>> BEGIN TASKS <<<")
    attributes = ",".join(tasks.attributes)
    task_log.debug(f"{attributes=}")
    for record in tasks.data:
        task_log.debug(f"{record}")
    task_log.debug(">>> END TASKS <<<")


def find_dot_or_die():
    # Check that the "dot" commandline utility is available
    import shutil
    if shutil.which("dot") is None:
        print(f"Error: {__name__} couldn't find the graphviz command line utility \"dot\" (see https://graphviz.org/download/).")
        print("Please install graphviz before continuing.")
        quit()


def interact(locals, g):
    import os
    import code
    import atexit
    import readline

    readline.parse_and_bind("tab: complete")

    histfile = os.path.join(os.path.expanduser("~"), ".otter_history")

    try:
        readline.read_history_file(histfile)
        numlines = readline.get_current_history_length()
    except FileNotFoundError:
        open(histfile, 'wb').close()
        numlines = 0

    atexit.register(append_history, numlines, histfile)

    k = ""
    for k, v in locals.items():
        if g is v:
            break

    banner = f"""
Graph {k} has {g.vcount()} nodes and {g.ecount()} edges

Entering interactive mode...
    """

    console = code.InteractiveConsole(locals=locals)
    console.interact(banner=banner, exitmsg=f"history saved to {histfile}")


def append_history(lines, file):
    import readline
    newlines = readline.get_current_history_length()
    readline.set_history_length(1000)
    readline.append_history_file(newlines - lines, file)
