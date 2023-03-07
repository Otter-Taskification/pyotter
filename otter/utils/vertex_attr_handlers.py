# TODO: re-implement any handlers with event model logic inside an event model

# TODO: ideas which need to be factored out of this module:
#  reduction operations for Iterable[V] -> V
#  wrapping reductions with log messages
#  validating reduction arguments
#  ==> these 3 operations should be separately composable for flexibility

from typing import Type, List, Iterable, Union, Protocol, TypeVar, Optional, Any
from .. import log
from ..log import DEBUG
from ..definitions import RegionType
from ..core import events
from ..utils import flatten
from loggingdecorators import on_init, on_call

get_module_logger = log.logger_getter("vertex.attr")

V = TypeVar("V")

class Reduction(Protocol[V]):
    """
    A callable which reduces an Iterable of Optional[V] to an Optional[V]
    """
    def __call__(self, collection: Iterable[Optional[V]]) -> Optional[V]: ...

# TODO: prefer CamelCase for classes, find better name
class ReductionDict(dict):
    """
    Maps strings to reduction operations
    """

    @on_init(logger=log.logger_getter("init_logger"))
    def __init__(self, keys: Iterable[str], default_reduction: Reduction=None, logger=None, level=DEBUG):
        self.log = logger or log.get_logger(self.__class__.__name__)
        self.level = level
        reduction: Reduction = default_reduction or pass_first_arg
        super().__init__({key: reduction for key in keys})

    def __setitem__(self, key: str, value: Reduction):
        self.log.log(self.level, f"set reduction '{value}' for string '{key}'", stacklevel=2)
        return super().__setitem__(key, value)


# TODO: prefer CamelCase for classes, find better name
# TODO: this class does too many things - decorates the handler to call, validates reduction arguments, applies handler
class LoggingValidatingReduction:
    """
    Apply a strategy for combining a set of values of some attribute
    Apply a given reduction operation to an iterable of values. Decorates the given reduction to log each time it is called.
    Validates the arguments to the reduction before applying the reduction.
    """

    @on_init(logger=log.logger_getter("init_logger"))
    def __init__(self, reduction: Reduction=None, accept: Union[Type, List[Type]]=None, msg="combining events"):
        """
        Wraps a handler function to allow checking and logging of the arguments passed to it.
        Log invocations of the handler with an on_call decorator.

        handler: a function which accepts one arg (a list of the values to be combined)
        accept: a type, or list of types, which each arg must conform to for the given handler to be applied
        """

        # TODO: if reduction is None, this doesn't actually perform a reduction - is that intentional?
        self.reduction = reduction if reduction else (lambda arg: arg)

        # TODO: remove reference to _Event once NewEvent fully adopted
        self.accept = accept or [list, events._Event, events.NewEvent]
        self.log = log.get_logger(self.__class__.__name__)

        # TODO: don't bake decorated reduction in, lift out and allow to be injected
        call_decorator = on_call(self.log, msg=msg)
        self.reduction = call_decorator(self.reduction)

    def __call__(self, args: List[Optional[V]]) -> Optional[V]:

        """
        Called by igraph.Graph.contract_vertices when combining a vertex attribute
        Expects args to be a list of values to be combined
        Returns the value to assign to the attribute of the vertex which replaces the contracted vertices
        If args is a list of lists, flatten into one list before passing to handler
        If args is a list with 1 item, return that item
        """

        assert isinstance(args, list)

        # Prevent any nesting of lists when args is a List[List[events._Event]]
        args = list(flatten(args))

        # TODO: this assertion should really be a unit test of flatten when called with nested lists
        assert isinstance(args, list) and all(not isinstance(item, list) for item in args) # flat list

        if all(arg is None for arg in args):
            self.log.debug(f"all args were None", stacklevel=4)
            return None

        # Filter out all values which are None (allow False to pass through)
        args = [arg for arg in args if arg is not None]

        if len(args) == 1:
            item = args[0]
            self.log.debug(f"list contains 1 item: {item}", stacklevel=4)
            # TODO: remove _Event api call and encapsulate requirement for events to always be inside a list
            return [item] if events.is_event(item) else item

        # Each arg must be of the given type (or one of the given list of types) for this handler
        A = isinstance(self.accept, Iterable) and all(isinstance(arg, tuple(self.accept)) for arg in args)
        B = isinstance(self.accept, type) and are(all, args, self.accept)
        if not (A or B):
            raise TypeError(f"expected {self.accept}, got {set(map(type, args))}")

        # args is list with > 1 element, none of which is None
        return self.reduction(args)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.reduction.__module__}.{self.reduction.__name__})"


### Strategies for reducing lists of attribute values to one value

# TODO: this function is poorly named. Maybe instances_of?
# TODO: consider removing as only used once
def are(reduce: Reduction[bool], args: List[Any], cls) -> bool:
    return reduce(isinstance(x, cls) for x in args)


def drop_args(args: Iterable[Any]) -> None:
    return None


def pass_args(args: List[Optional[V]]) -> List[Optional[V]]:
    return args


def pass_first_arg(args: List[Optional[V]]) -> Optional[V]:
    return args[0]


# TODO: perhaps this handler is too specific to need defining here - consider defining in-line in application code.
# TODO: as it's only used in 1 place
def pass_bool_value(values: List[Optional[bool]]) -> None:
    """
    Given a list containing Optional[bool], where all boolean values are the same, return the single boolean value.
    """
    # TODO: An `assert` would be more explicit than this `if` since we raise if the check fails anyway.
    # assert set(values) in [{True, None}, {False, None}]
    # assert set(map(type, values)) == {bool, type(None)}
    if set(map(type, values)) == {bool, type(None)}:
        A, B = set(values)
        # return the non-None boolean
        return A if B is None else B
    raise NotImplementedError(f"not implemented for {values=}")


def pass_the_unique_value(args):
    items = set(args)
    if len(items) == 1:
        return items.pop()
    raise ValueError(f"expected one unique item: {items=}")


def pass_the_set_of_values(args):
    items = set(args)
    if len(items) == 1:
        return items.pop()
    else:
        return items


def _return_unique_event(args, region_type):
    """
    Expects args to be one of:
        - List[events._Event]
        - List[List[events._Event]]
    Reduce the argument down to a List[events._Event] first
    Expects to find & return exactly 1 single-executor event in this list
    Error otherwise
    """
    assert events.is_event_list(args)

    # args is guaranteed to be a list of events
    unique_events = set(e for e in args if e.region_type == region_type)

    logger = get_module_logger()
    if len(unique_events) == 1:
        event = unique_events.pop()
        logger.debug(f"returning event: {event}")
        result = [event]
    elif len(unique_events) == 0:
        event_types = set(type(e).__name__ for e in args)
        logger.debug(f"no {region_type} events, returning event list ({event_types=})")
        result = args
    else:
        # error: if >1 event arrived here, the vertices were somehow mis-labelled
        logger.error(f"multiple {region_type} events received: {args}")
        raise ValueError(f"multiple {region_type} events received: {args}")
    
    assert events.is_event_list(result)
    return result


def return_unique_single_executor_event(args):
    assert events.is_event_list(args)
    result = _return_unique_event(args, RegionType.single_executor)
    assert events.is_event_list(result)
    return result


def return_unique_master_event(args):
    assert events.is_event_list(args)
    result = _return_unique_event(args, RegionType.master)
    assert events.is_event_list(result)
    return result

def return_unique_taskswitch_complete_event(args):
    assert events.is_event_list(args)
    # TODO: remove _Event api call
    assert all(event.is_task_switch_event for event in args)
    event_set = set(event for event in args if event.is_task_switch_complete_event)
    assert len(event_set) == 1
    return event_set.pop()

def return_unique_taskgroup_complete_event(args):
    assert events.is_event_list(args)
    # TODO: remove _Event api call
    assert all(event.is_task_group_end_event for event in args)
    event_set = set(args)
    assert len(event_set) == 1
    return event_set.pop()

def reject_task_create(args):
    logger = get_module_logger()
    # TODO: remove _Event api call
    events_filtered = [event for event in args if not event.is_task_create_event]
    if len(events_filtered) == 0:
        raise NotImplementedError("No events remain after filtering")
    n_args = len(args)
    n_accept = len(events_filtered)
    logger.debug(f"return {n_accept}/{n_args}: {events_filtered}")
    return events_filtered
