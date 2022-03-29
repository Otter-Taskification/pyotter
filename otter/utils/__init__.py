from ..logging import get_logger
from .counters import PrettyCounter, VertexLabeller
from .vertex_predicates import key_is_not_none, is_region_type, is_empty_task_region
from .vertex_attr_handlers import drop_args, unique_arg, unique_or_none, make_event_combiner
from . import decorate
from . import vertex_attr_handlers as handlers

def default_attribute_handler(names, logger=None):
    if logger is None:
        logger = get_logger()
    h = dict()
    for name in names:
        h[name] = decorate.log_call_with_msg(drop_args, f"combining attribute: {name}", logger)
        logger.info(f"({__package__}) set attribute handler '{drop_args.__module__}.{drop_args.__name__}' for vertex attribute '{name}'")
    return h
