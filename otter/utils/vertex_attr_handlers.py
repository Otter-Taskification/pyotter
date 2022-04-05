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


def are(reduce, args, T):
    return reduce(isinstance(x, T) for x in args)


# @on_call(module_logger)
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
    is_event_list = lambda this : isinstance(this, list) and all(isinstance(thing, events._Event) for thing in this)
    assert is_event_list(args)
    if not (isinstance(args, list) and len(args) > 0):
        module_logger.debug(f"{args}")
        for item in args:
            module_logger.debug(f"    {item=}")
        raise TypeError("!!!")

    if not is_event_list(args):
        assert all(is_event_list(item) for item in args)
        module_logger.debug(f"detected list of event_list, chaining {len(args)} lists together")
        module_logger.debug(f"before: len={list(map(len, args))}")
        args = list(chain(*args))
        module_logger.debug(f"after: 1 event list of len={len(args)}")
        assert is_event_list(args)
    else:
        module_logger.debug(f"detected event_list len={len(args)}, types={set(map(type, args))}")

    # args is guaranteed to be a list of events
    unique_events = set(e for e in args if e.region_type == region_type)

    if len(unique_events) > 1:
        # error: if >1 event arrived here, the vertices were somehow mis-labelled
        module_logger.error(f"multiple {region_type} events received: {args}")
        raise ValueError(f"multiple {region_type} events received: {args}")
    elif len(unique_events) == 1:
        event = unique_events.pop()
        module_logger.debug(f"returning event: {event}")
        return event
    else:
        event_types = set(type(e).__name__ for e in args)
        module_logger.debug(f"no {region_type} events, returning event list ({event_types=})")
        return args


def return_unique_single_executor_event(args):
    return _return_unique_event(args, RegionType.single_executor)


def return_unique_master_event(args):
    return _return_unique_event(args, RegionType.single_executor)


# @on_call(module_logger)
def reject_task_create(args):
    events_filtered = [event for event in args if not event.is_task_create_event]
    if len(events_filtered) == 1:
        return events_filtered[0]
    elif len(events_filtered) == 0:
        raise NotImplementedError("No events remain after filtering")
    else:
        return events_filtered


class AttributeHandlerTable(dict):

    @on_init(logger=get_logger("init_logger"))
    def __init__(self, names, default_handler=pass_first_arg, logger=None, level=DEBUG):
        self.log = logger or get_logger(self.__class__.__name__)
        self.level = level
        super().__init__({name: default_handler for name in names})

    def __setitem__(self, event, handler):
        self.log.log(self.level, f"set handler '{handler}' for vertex attribute '{event}'", stacklevel=2)
        return super().__setitem__(event, handler)


class VertexAttributeCombiner:

    @on_init(logger=get_logger("init_logger"))
    def __init__(self, handler=None, cls: Union[Type, List[Type]]=[list, events._Event], msg="combining events"):
        self.handler = handler if handler else (lambda arg: arg)
        self.log = get_logger(self.__class__.__name__)
        self.combine_values = self.make(handler, self.log, cls, msg=msg, depth=1)

    def __call__(self, args):
        """Call the value combiner which applies the specified handlers"""
        return self.combine_values(args)

    @staticmethod
    def make(handler, logger, cls, msg: str="", depth=0):
        """
        Using value=1, log messages emitted within _value_combiner will get the filename & lineno of the caller of
        _value_combiner. For each additional wrapper around _value_combiner, increase the depth argument by 1 to get
        the correct stack information in the log messages.
        """
        @wraps(handler)
        def _value_combiner(args):
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
            assert isinstance(args, list) and all(not isinstance(item, list) for item in args)

            if all(arg is None for arg in args):
                logger.debug(f"check None: all args were None")
                return None

            # Filter out all values which are None (allow False to pass through)
            args = list(filter(lambda x : x is not None, args))

            if len(args) == 1:
                item = args[0]
                logger.debug(f"list contains 1 item: {item}")
                return item
            # args is list with > 1 element, none of which is None

            # Each arg must match the given type (or one of the given list of types) for this handler
            A = isinstance(cls, Iterable) and all(isinstance(arg, tuple(cls)) for arg in args)
            B = isinstance(cls, type) and are(all, args, cls)
            if not (A or B):
                raise TypeError(f"expected {cls}, got {set(map(type, args))}")

            return handler(args)

        # Apply the on_call decorator with the correct depth so that logs dispatched inside nested decorators get the
        # correct stacklevel to find the original caller
        return on_call(logger, msg=msg, depth=depth)(_value_combiner)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.handler.__module__}.{self.handler.__name__})"
