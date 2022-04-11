from collections import defaultdict
from . import log

get_module_logger = log.logger_getter("styling")

# Map region type to node color
colormap_region_type = defaultdict(lambda: 'grey', **{
    'initial_task': 'green',
    'implicit_task': 'fuchsia',
    'explicit_task': 'cyan',
    'parallel': 'yellow',
    'single_executor': 'blue',
    'single_other': 'orange',
    'taskwait': 'red',
    'taskgroup': 'purple',
    'barrier_implicit': 'darkgreen',
    'master': 'magenta',

    # Workshare regions
    'loop': 'brown',
    'taskloop': 'orange',

    # For colouring by endpoint
    'enter': 'green',
    'leave': 'red'
})

colormap_edge_type = defaultdict(lambda: 'black', **{
    'taskwait': 'red',
    'taskgroup': 'red',
})

shapemap_region_type = defaultdict(lambda: 'circle', **{
    'initial_task': 'square',
    'implicit_task': 'square',
    'explicit_task': 'square',
    'parallel': 'parallelogram',

    # Sync regions
    'taskwait': 'octagon',
    'taskgroup': 'octagon',
    'barrier_implicit': 'octagon',

    # Workshare regions
    'loop': 'diamond',
    'taskloop': 'diamond',
    'single_executor': 'diamond',

    # Master
    'master': 'circle'
})

task_attribute_names = {
    "id": "Unique ID",
    "parent_id": "Parent ID",
    "task_type": "Task Type",
    "crt_ts": "Creation Time",
    "end_ts": "End Time",
    # "duration": "Duration",
    "exclusive_duration": "Exclusive Duration",
    "inclusive_duration": "Inclusive Duration"
}

colormap_task_type = defaultdict(lambda: 'black', **{
    'initial_task': 'cyan',
    'implicit_task': 'gray',
    'explicit_task': 'red',
    'target_task': 'orange'
})

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

def style_graph(graph):
    logger = get_module_logger()
    logger.info(f"styling graph:")
    for line in str(graph).split("\n"):
        logger.info(f"{line}")

    rtype = graph.vs['region_type']
    graph.vs['style'] = "filled"
    graph.vs['shape'] = [shapemap_region_type[key] for key in rtype]
    graph.vs['color'] = [colormap_region_type[key] for key in rtype]
