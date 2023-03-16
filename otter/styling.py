from typing import Tuple
from igraph import Graph
from . import log
from . import definitions as defn
from . import graph_styling

get_module_logger = log.logger_getter("styling")

# Map region type to node color
colormap_region_type = { # default grey
    'initial_task': 'green',
    'implicit_task': 'fuchsia',
    'explicit_task': 'cyan',
    'parallel': 'yellow',
    'single_executor': 'blue',
    'single_other': 'orange',
    'taskwait': 'red',
    'taskgroup': 'purple',
    'barrier_implicit': 'darkgreen',
    'barrier_explicit': '#06A4A9',    # teal
    'master': 'magenta',
    'generic_phase': 'fuchsia',

    # Workshare regions
    'loop': 'brown',
    'taskloop': 'orange',

    # For colouring by endpoint
    'enter': 'green',
    'leave': 'red'
}

colormap_edge_type = { # default black
    'taskwait': 'red',
    'taskgroup': 'red',
}

shapemap_region_type = { # default to circle
    'initial_task': 'square',
    'implicit_task': 'square',
    'explicit_task': 'square',
    'parallel': 'parallelogram',

    # Sync regions
    'taskwait': 'octagon',
    'taskgroup': 'octagon',
    'barrier_implicit': 'octagon',
    'barrier_explicit': 'octagon',

    # Workshare regions
    'loop': 'diamond',
    'taskloop': 'diamond',
    'single_executor': 'diamond',

    # Master
    'master': 'circle',

    # Phases
    'generic_phase': 'box'
}

task_attribute_names = { # default to "UNDEFINED"
    "id": "Unique ID",
    "parent_id": "Parent ID",
    "task_type": "Task Type",
    "crt_ts": "Creation Time",
    "end_ts": "End Time",
    "exclusive_duration": "Exclusive Duration",
    "inclusive_duration": "Inclusive Duration",
    "num_children": "Child Tasks",
    "num_descendants": "Descendant Tasks",
    "start_ts": "Start Time",
    "source_file_name": "Source File",
    "source_func_name": "Source Function",
    "source_line_number": "Source Line No."
}

colormap_task_type = { # default to black
    'initial_task': 'cyan',
    'implicit_task': 'gray',
    'explicit_task': 'red',
    'target_task': 'orange'
}

def style_tasks(t):
    logger = get_module_logger()
    logger.info(f"styling task_tree:")
    for line in str(t).split("\n"):
        logger.info(f"{line}")
    logger.info(f"attributes:")
    attributes = t.vs.attribute_names()
    logger.info(f"{attributes=}")
    for name in attributes:
        logger.info(f"{name}")

    tasks = t.vs['task']
    t.vs['style'] = 'filled'
    t.vs['shape'] = 'square'
    t.vs['color'] = [colormap_task_type[t.task_type] for t in tasks]


class StyleVertexShapeAsRegionType(graph_styling.BaseGraphStyle):

    def get_vertex_style(self, vertex) -> Tuple[graph_styling.VertexStyle, str]:
        label = vertex['vertex_label'] or " "
        style = graph_styling.VertexStyle(
            "filled",
            shapemap_region_type.get(vertex["vertex_color_key"], "circle"),
            colormap_region_type.get(vertex["vertex_shape_key"], "fuchsia")
        )
        return style, label

def style_graph(graph: Graph, style: graph_styling.GraphStylingProtocol = StyleVertexShapeAsRegionType()):

    assert(defn.Attr.edge_type in graph.es.attribute_names())

    logger = get_module_logger()
    logger.info(f"styling graph:")
    for line in str(graph).split("\n"):
        logger.info(f"{line}")

    if 'name' in graph.vs.attribute_names():
        raise ValueError()

    if "label" in graph.vs.attribute_names():
        logger.warn("vertex labels already defined, not overwritten")
    else:
        logger.debug("defining vertex labels")
        graph.vs['label'] = [v['vertex_label'] or " " for v in graph.vs]

    per_vertex_styles_labels = [style.get_vertex_style(vertex) for vertex in graph.vs]
    vertex_styles_tuples, vertex_labels = list(map(list, zip(*per_vertex_styles_labels)))
    vertex_styles, vertex_shapes, vertex_colors = list(map(list, zip(*vertex_styles_tuples)))
    graph.vs['style'] = vertex_styles
    graph.vs['shape'] = vertex_shapes
    graph.vs['color'] = vertex_colors

    # graph.vs['style'] = "filled"
    # graph.vs['shape'] = [shapemap_region_type[key] for key in graph.vs["vertex_color_key"]]
    # graph.vs['color'] = [colormap_region_type[key] for key in graph.vs["vertex_shape_key"]]
    # graph.es['color'] = [colormap_edge_type[key] for key in graph.es[defn.Attr.edge_type]]
