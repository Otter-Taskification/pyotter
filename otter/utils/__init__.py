from .counters import label_groups_if
from .iterate import pairwise, flatten, transpose_list_to_dict
from .vertex_predicates import key_is_not_none, \
    is_region_type, \
    is_empty_task_region, \
    is_terminal_task_vertex, \
    is_task_group_end_vertex, \
    is_single_executor, \
    is_master, \
    is_taskwait
from .edge_predicates import edge_connects_same_type
from .vertex_attr_handlers import combine_attribute_strategy, strategy_lookup
from . import vertex_attr_handlers as handlers
from ..log import get_logger

def dump_to_log_file(chunks, tasks):
    chunk_log = get_logger("chunks_debug")
    graph_log = get_logger("graphs_debug")
    task_log = get_logger("tasks_debug")

    graph_log.debug(">>> BEGIN GRAPHS <<<")
    chunk_log.debug(f">>> BEGIN CHUNKS <<<")

    for chunk in chunks:

        # write chunk
        for line in chunk.to_text():
            chunk_log.debug(f"{line}")

        # write graph
        graph_log.debug(f"Chunk type: {chunk.type}")
        g = chunk.graph
        lines = [" ".join(f"{g}".split("\n"))]
        for line in lines:
            graph_log.debug(f"{line}")
        for v in g.vs:
            graph_log.debug(f"{v}")
        graph_log.debug("")

    chunk_log.debug(f">>> END CHUNKS <<<")
    graph_log.debug(">>> END GRAPHS <<<")

    task_log.debug(">>> BEGIN TASKS <<<")
    attributes = ",".join(tasks.attributes)
    task_log.debug(f"{attributes=}")
    for record in tasks.data:
        task_log.debug(f"{record}")
    task_log.debug(">>> END TASKS <<<")
