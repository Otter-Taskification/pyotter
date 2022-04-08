from .attributes import colormap_region_type, colormap_edge_type, shapemap_region_type, task_attribute_names, colormap_task_type
from ..log import get_logger

module_logger = get_logger("styling")

def style_tasks(t):
    module_logger.info(f"styling task_tree:")
    for line in str(t).split("\n"):
        module_logger.info(f"{line}")
    module_logger.info(f"attributes:")
    attributes = t.vs.attribute_names()
    module_logger.info(f"{attributes=}")
    for name in attributes:
        module_logger.info(f"{name}")

    tasks = t.vs['task']
    t.vs['style'] = 'filled'
    t.vs['shape'] = 'square'
    t.vs['color'] = [colormap_task_type[t.task_type] for t in tasks]

def style_graph(graph):
    module_logger.info(f"styling graph:")
    for line in str(graph).split("\n"):
        module_logger.info(f"{line}")

    rtype = graph.vs['region_type']
    graph.vs['style'] = "filled"
    graph.vs['shape'] = [shapemap_region_type[key] for key in rtype]
    graph.vs['color'] = [colormap_region_type[key] for key in rtype]
