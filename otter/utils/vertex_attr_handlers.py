from collections.abc import Iterable
from functools import wraps
from itertools import chain
from typing import Type, List, Union
from .. import log
from ..log import get_logger, DEBUG, INFO
from ..definitions import RegionType
from ..EventFactory import events
from ..utils import flatten
from loggingdecorators import on_init, on_call

module_logger = get_logger("vertex.attr")


class AttributeHandlerTable(dict):

    @on_init(logger=get_logger("init_logger"))
    def __init__(self, names, default_handler=None, logger=None, level=DEBUG):
        """
        dict which maps attribute names to handlers for combining that attribute
        overrides __setitem__ to log each time a handler is set
        """
        self.log = logger or get_logger(self.__class__.__name__)
        self.level = level
        handler = default_handler or pass_first_arg
        super().__init__({name: handler for name in names})

    def __setitem__(self, event, handler):
        self.log.log(self.level, f"set handler '{handler}' for vertex attribute '{event}'", stacklevel=2)
        return super().__setitem__(event, handler)


class VertexAttributeCombiner:

    @on_init(logger=get_logger("init_logger"))
    def __init__(self, handler=None, accept: Union[Type, List[Type]]=[list, events._Event], msg="combining events"):
        """
        Wraps a handler function to allow checking and logging of the arguments passed to it.
        Log invocations of the handler with an on_call decorator.

        handler: a function which accepts one arg (a list of the values to be combined)
        accept: a type, or list of types, which each arg must conform to for the given handler to be applied
        """
        self.handler = handler if handler else (lambda arg: arg)
        self.accept = accept
        self.log = get_logger(self.__class__.__name__)
        call_decorator = on_call(self.log, msg=msg)
        self.handler = call_decorator(self.handler)

    def __call__(self, args):

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
        assert isinstance(args, list) and all(not isinstance(item, list) for item in args) # flat list

        if all(arg is None for arg in args):
            self.log.debug(f"all args were None", stacklevel=4)
            return None

        # Filter out all values which are None (allow False to pass through)
        args = list(filter(lambda x: x is not None, args))

        if len(args) == 1:
            item = args[0]
            self.log.debug(f"list contains 1 item: {item}", stacklevel=4)
            return item

        # Each arg must be of the given type (or one of the given list of types) for this handler
        A = isinstance(self.accept, Iterable) and all(isinstance(arg, tuple(self.accept)) for arg in args)
        B = isinstance(self.accept, type) and are(all, args, self.accept)
        if not (A or B):
            raise TypeError(f"expected {accept}, got {set(map(type, args))}")

        # args is list with > 1 element, none of which is None
        return self.handler(args)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.handler.__module__}.{self.handler.__name__})"


### Handlers

def are(reduce, args, T):
    return reduce(isinstance(x, T) for x in args)


def drop_args(args):
    return None


def pass_args(args):
    return args


def pass_first_arg(args):
    return args[0]


def pass_bool_value(values):
    if set(map(type, values)) == {bool, type(None)}:
        A, B = set(values)
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
    is_event_list = lambda this : all(isinstance(thing, events._Event) for thing in this)
    assert isinstance(args, list)
    assert is_event_list(args)

    # args is guaranteed to be a list of events
    unique_events = set(e for e in args if e.region_type == region_type)

    if len(unique_events) == 1:
        event = unique_events.pop()
        module_logger.debug(f"returning event: {event}")
        return event
    elif len(unique_events) == 0:
        event_types = set(type(e).__name__ for e in args)
        module_logger.debug(f"no {region_type} events, returning event list ({event_types=})")
        return args
    else:
        # error: if >1 event arrived here, the vertices were somehow mis-labelled
        module_logger.error(f"multiple {region_type} events received: {args}")
        raise ValueError(f"multiple {region_type} events received: {args}")


def return_unique_single_executor_event(args):
    return _return_unique_event(args, RegionType.single_executor)


def return_unique_master_event(args):
    return _return_unique_event(args, RegionType.single_executor)

def return_unique_taskswitch_complete_event(args):
    assert isinstance(args, list)
    assert all(events.is_event(item) for item in args)
    assert all(event.is_task_switch_event for event in args)
    event_set = set(event for event in args if event.is_task_switch_complete_event)
    assert len(event_set) == 1
    return event_set.pop()

def reject_task_create(args):
    events_filtered = [event for event in args if not event.is_task_create_event]
    if len(events_filtered) == 1:
        return events_filtered[0]
    elif len(events_filtered) == 0:
        raise NotImplementedError("No events remain after filtering")
    else:
        return events_filtered
