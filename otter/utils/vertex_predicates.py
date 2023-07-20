from ..definitions import RegionType, EventType
from ..core import events
from .. import log
from .decorators import warn_deprecated
from typing import Callable

get_module_logger = log.logger_getter("vertex.pred")


# TODO: this is definitely a predicate applied to a vertex
def key_is_not_none(key: str) -> Callable:
    # return lambda vertex: vertex[key] is not None
    def check_key(vertex):
        try:
            return vertex[key] is not None
        except KeyError as e:
            # KeyError indicates we are looking for a key that wasn't defined in the graph, so fail loudly
            get_module_logger().error(f"exception: {e} for {key=}")
            raise e

    return check_key


# TODO: predicate applies to events, extract to event model
def _is_event_region_type(event, region_type) -> bool:
    assert events.is_event(event)
    # TODO: remove coupling to event API
    return (
        event.is_enter_event or event.is_leave_event
    ) and event.region_type == region_type


# TODO: predicate applies to events, extract to event model
@warn_deprecated
def is_region_type(region_type: RegionType) -> Callable:
    # Return a function which checks whether the event attribute of a vertex is of the given region type
    def check_region_type(vertex):
        # Check whether all Events in the vertex['event'] list have the corresponding
        # region_type.
        assert events.is_event_list(vertex["event"])
        return all(_is_event_region_type(e, region_type) for e in vertex["event"])

    return check_region_type


# TODO: predicate applies to events, extract to event model. Also refers to private vertex attributes
@warn_deprecated
def is_empty_task_region(vertex) -> bool:
    # Return True if vertex is a task-enter (-leave) node with no outgoing (incoming) edges
    if vertex["_task_cluster_id"] is None:
        return False
    if vertex["_is_task_enter_node"] or vertex["_is_task_leave_node"]:
        return (vertex["_is_task_leave_node"] and vertex.indegree() == 0) or (
            vertex["_is_task_enter_node"] and vertex.outdegree() == 0
        )
    if type(vertex["event"]) is list and set(map(type, vertex["event"])) in [
        {EventType.task_switch}
    ]:
        return (all(vertex["_is_task_leave_node"]) and vertex.indegree() == 0) or (
            all(vertex["_is_task_enter_node"]) and vertex.outdegree() == 0
        )


# TODO: predicate applies to events, extract to event model
@warn_deprecated
def is_terminal_task_vertex(vertex) -> bool:
    event = vertex["event"]
    assert events.is_event_list(event)
    return any(e.is_task_switch_complete_event for e in event)


# TODO: predicate applies to events, extract to event model
@warn_deprecated
def is_task_group_end_vertex(vertex) -> bool:
    assert events.is_event_list(vertex["event"])
    return all(e.is_task_group_end_event for e in vertex["event"])


is_single_executor = warn_deprecated(is_region_type(RegionType.single_executor))
is_master = warn_deprecated(is_region_type(RegionType.master))
is_taskwait = warn_deprecated(is_region_type(RegionType.taskwait))
