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
