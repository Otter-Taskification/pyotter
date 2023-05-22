from typing import Tuple
from igraph import Graph
from . import log
from . import definitions as defn
from . import graph_styling

get_module_logger = log.logger_getter("styling")

# Map region type to node color
colormap_region_type = { # default grey
    'initial_task': 'green',
    'implicit_task': 'grey',
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

colormap_task_flavour = {
    '0': 'green',
    '1': 'orange',
    '2': 'blue',
    '3': 'yellow'
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
        return graph_styling.VertexStyle(
            "filled",
            shapemap_region_type.get(vertex["vertex_shape_key"], "circle"),
            colormap_region_type.get(vertex["vertex_color_key"], "fuchsia"),
            vertex['vertex_label'] or " "
        )


class StyleVertexShapeAsRegionTypeAndColourAsTaskFlavour(graph_styling.BaseGraphStyle):

    def get_vertex_style(self, vertex) -> Tuple[graph_styling.VertexStyle, str]:
        default_colour = "fuchsia"
        task_flavour = vertex["task_flavour"]
        if task_flavour is not None:
            colour = colormap_task_flavour.get(task_flavour, default_colour)
        else:
            colour = colormap_region_type.get(vertex["vertex_color_key"], default_colour)
        return graph_styling.VertexStyle(
            "filled",
            shapemap_region_type.get(vertex["vertex_shape_key"], "circle"),
            colour,
            vertex['vertex_label'] or " "
        )

def style_graph(graph: Graph, style: graph_styling.GraphStylingProtocol = StyleVertexShapeAsRegionType()):
    per_vertex_styles_labels = [style.get_vertex_style(vertex) for vertex in graph.vs]
    vertex_styles, vertex_shapes, vertex_colors, vertex_labels = list(map(list, zip(*per_vertex_styles_labels)))
    graph.vs['style'] = vertex_styles
    graph.vs['shape'] = vertex_shapes
    graph.vs['color'] = vertex_colors
    graph.vs['label'] = vertex_labels
