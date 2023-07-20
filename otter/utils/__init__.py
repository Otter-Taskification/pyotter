from collections import Counter
from .counters import SequenceLabeller, CountingDict
from .iterate import pairwise, flatten, transpose_list_to_dict, batched
from . import vertex_predicates as vpred
from . import edge_predicates as epred
from .vertex_attr_handlers import LoggingValidatingReduction, ReductionDict
from . import vertex_attr_handlers as handlers
from ..log import get_logger
from .decorators import warn_deprecated
from igraph import Graph


def dump_to_log_file(chunks, graphs, tasks, where=None):
    from os import path
    from ..reporting.report import save_graph_to_dot, convert_to_svg

    chunk_log = get_logger("chunks_debug")
    graph_log = get_logger("graphs_debug")
    task_log = get_logger("tasks_debug")

    graph_log.debug(">>> BEGIN GRAPHS <<<")
    chunk_log.debug(f">>> BEGIN CHUNKS <<<")

    for k, (chunk, graph) in enumerate(zip(chunks, graphs)):
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

        # write graph as a dot file and convert to svg
        dotfile = f"graph_{k}.dot"
        svgfile = f"graph_{k}.svg"
        if where is not None:
            dotfile = path.join(where, dotfile)
            svgfile = path.join(where, svgfile)
        save_graph_to_dot(graph, dotfile)
        convert_to_svg(dotfile, svgfile)

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
        print(
            f'Error: {__name__} couldn\'t find the graphviz command line utility "dot" (see https://graphviz.org/download/).'
        )
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
        open(histfile, "wb").close()
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


def assert_vertex_event_list(graph: Graph) -> None:
    from ..core.events import Event

    for vertex in graph.vs:
        event_list = vertex["event_list"]
        assert isinstance(event_list, list)
        assert all(isinstance(item, Event) for item in event_list)


def dump_graph_to_file(
    graph: Graph, filename: str = "graph.log", no_flatten: list = None
) -> None:
    log = get_logger("main")
    log.info(f"writing graph to {filename}")
    with open(filename, "w") as f:
        f.write("### VERTEX ATTRIBUTES:\n")
        for name in graph.vs.attribute_names():
            levels = set(flatten(graph.vs[name], exclude=no_flatten))
            n_levels = len(levels)
            if n_levels <= 6:
                f.write(f"  {name:>35} {n_levels:>6} levels {list(levels)}\n")
            else:
                f.write(f"  {name:>35} {n_levels:>6} levels (...)\n")

        try:
            region_type_count = Counter(graph.vs["region_type"])
            region_types = (
                "\n".join([f"{region_type_count[k]:>6} {k}" for k in region_type_count])
                + f"\nTotal count: {sum(region_type_count.values())}"
            )
        except KeyError as e:
            region_types = f"{e}"

        f.write("\nCount of vertex['region_type'] values:\n")
        f.write(region_types)
        f.write("\n\n")

        f.write("### EDGE ATTRIBUTES:\n")
        for name in graph.es.attribute_names():
            levels = set(flatten(graph.es[name]))
            n_levels = len(levels)
            if n_levels <= 6:
                f.write(f"  {name:>35} {n_levels:>6} levels ({list(levels)})\n")
            else:
                f.write(f"  {name:>35} {n_levels:>6} levels (...)\n")

        f.write("\n")

        f.write("### VERTICES:\n")
        for v in graph.vs:
            f.write(f"{v}\n")

        f.write("\n")
        f.write("### EDGES:\n")
        for e in graph.es:
            f.write(f"{e.tuple}\n")
