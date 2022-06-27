from ..definitions import RegionType, EventType
from ..core import events
from .. import log
from typing import Callable

get_module_logger = log.logger_getter("vertex.pred")

def key_is_not_none(key) -> Callable:
    # return lambda vertex: vertex[key] is not None
    def check_key(vertex):
        try:
            return vertex[key] is not None
        except KeyError as e:
            get_module_logger().error(f"exception: {e} for {key=}")
            raise e
    return check_key

def _is_event_region_type(event, region_type) -> bool:
    assert events.is_event(event)
    return (event.is_enter_event or event.is_leave_event) and event.region_type == region_type

def is_region_type(region_type: RegionType) -> Callable:
    # Return a function which checks whether the event attribute of a vertex is of the given region type
    def check_region_type(vertex):
        # Check whether all Events in the vertex['event'] list have the corresponding
        # region_type.
        assert events.is_event_list(vertex['event'])
        return all(_is_event_region_type(e, region_type) for e in vertex['event'])
    return check_region_type

def is_empty_task_region(vertex) -> bool:
    # Return True if vertex is a task-enter (-leave) node with no outgoing (incoming) edges
    if vertex['_task_cluster_id'] is None:
        return False
    if vertex['_is_task_enter_node'] or vertex['_is_task_leave_node']:
        return ((vertex['_is_task_leave_node'] and vertex.indegree() == 0) or
                (vertex['_is_task_enter_node'] and vertex.outdegree() == 0))
    if type(vertex['event']) is list and set(map(type, vertex['event'])) in [{EventType.task_switch}]:
        return ((all(vertex['_is_task_leave_node']) and vertex.indegree() == 0) or
                (all(vertex['_is_task_enter_node']) and vertex.outdegree() == 0))

def is_terminal_task_vertex(vertex) -> bool:
    event = vertex['event']
    assert events.is_event_list(event)
    return any(e.is_task_switch_complete_event for e in event)

def is_task_group_end_vertex(vertex) -> bool:
    assert events.is_event_list(vertex['event'])
    return all(e.is_task_group_end_event for e in vertex['event'])

is_single_executor = is_region_type(RegionType.single_executor)
is_master = is_region_type(RegionType.master)
is_taskwait = is_region_type(RegionType.taskwait)
