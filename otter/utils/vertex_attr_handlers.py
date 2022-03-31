from functools import wraps
from itertools import chain
from logging import DEBUG, INFO
from ..logging import get_logger
from ..definitions import RegionType
from ..EventFactory import events
from otter.decorators import log_init, log_args, log_msg

module_logger = get_logger(f"{__name__}")


def are(reduce, args, T):
    return reduce(isinstance(x, T) for x in args)


@log_args(module_logger)
def drop_args(args):
    return None


def pass_the_unique_value(args):
    items = set(args)
    if len(items) == 1:
        return items.pop()
    raise ValueError(f"expected single item: {items=}")


def _pass_unique_event(args, region_type: RegionType):
    """
    Given a List[events._Event], return a single-exec event from the set of such events.
    If no such events present, return the original list.
    When combining events, it is an error if >1 single-exec event is encountered for a given vertex
    """
    assert isinstance(args, list)
    assert are(all, args, events._Event)
    if len(args) == 1:
        raise RuntimeError(f"shouldn't pass list of 1 event: {args}")
    # args is a list of >1 events._Event
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


def pass_unique_single_executor(args):
    return _pass_unique_event(args, RegionType.single_executor)


def pass_unique_master_event(args):
    return _pass_unique_event(args, RegionType.master)


@log_args(module_logger)
def reject_task_create(args):
    events_filtered = [event for event in args if event.is_task_create_event]
    if len(events_filtered) == 1:
        return events_filtered[0]
    elif len(events_filtered) == 0:
        raise NotImplementedError("No events remain after filtering")
    else:
        return events_filtered


class AttributeHandlerTable(dict):

    @log_init()
    def __init__(self, names, handler=drop_args, logger=None, level=DEBUG):
        self.log = logger or get_logger(f"{self.__class__.__name__}")
        self.level = level
        super().__init__({name: log_msg(handler, f"combining attribute {name} with handler {handler}", self.log) for name in names})

    def __setitem__(self, event, handler):
        self.log.log(self.level, f"set handler '{handler}' for vertex attribute '{event}'")
        return super().__setitem__(event, handler)


class VertexAttributeCombiner:

    @log_init()
    def __init__(self, handler, list_handler=None, logger=None, level=DEBUG, cls=events._Event, msg="combining events"):
        self.handler = handler
        self.log = logger or get_logger(self.__class__.__name__)
        self.combine_values = self.make(handler, list_handler, self.log, cls=cls)
        if level is not None:
            self.combine_values = log_msg(self.combine_values, msg, self.log, level=level)

    def __call__(self, args):
        """Call the value combiner which applies the specified handlers"""
        return self.combine_values(args)

    @staticmethod
    def make(handler, list_handler, logger, cls):
        @log_args(logger)
        @wraps(handler)
        def _value_combiner(args):
            """
            Called by igraph.Graph.contract_vertices when combining a vertex attribute
            "args" is a list of values to be combined
            Returns the value to assign to the attribute of the vertex which replaces the contracted vertices
            If args is a list with 1 item, return that item
            If args is a list of events, pass to handler
            If args is a list of lists, pass to list handler (default: chain lists together to prevent nested lists)
            No other types are expected to be passed here via the 'event' vertex attribute
            """
            assert isinstance(args, list)
            if all(arg is None for arg in args):
                logger.debug(f"check None: all args were None")
                return None
            if any(arg is None for arg in args):
                logger.debug(f"check None: at least 1 arg was None")
            else:
                logger.debug(f"check None: no args were None")
            # args is a list in which fewer than all items are None i.e. at least 1 item which is not None
            if len(args) == 1:
                item = args[0]
                logger.debug(f"list contains 1 item: {item}")
                return item
            # args is list with > 1 element, at least 1 of which is not None
            if are(all, args, cls): # doesn't handle the case for List[None, MasterBegin], for example...
                return handler(args)
            elif are(all, args, list):
                return list_handler(args) if list_handler else list(chain(*args))
            elif are(any, args, type(None)):
                logger.error(args)
                raise TypeError(f"mixed type(s) with NoneType: {set(map(type, args))}")
            else:
                logger.error(args)
                raise TypeError(f"unexpected type(s): {set(map(type, args))}")
        return _value_combiner

    def __repr__(self):
        return f"{self.__class__.__name__}({self.handler.__module__}.{self.handler.__name__})"
