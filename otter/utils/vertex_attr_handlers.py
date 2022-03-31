from functools import wraps
from itertools import chain
from logging import DEBUG, INFO
from ..logging import get_logger
from ..definitions import RegionType
from ..EventFactory import events
from .decorate import log_init, log_args, log_msg

module_logger = get_logger(f"{__name__}")


def are(reduce, args, T):
    return reduce(isinstance(x, T) for x in args)


@log_args(module_logger)
def drop_args(args):
    return None


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



def pass_single_executor(args):
    return _pass_unique_event(args, RegionType.single_executor)


def pass_unique_master_event(args):
    return _pass_unique_event(args, RegionType.master)


class AttributeHandlerTable(dict):

    @log_init()
    def __init__(self, names, handler=drop_args, logger=None, level=DEBUG):
        self.log = logger or get_logger(f"{self.__class__.__name__}")
        self.level = level
        super().__init__({name: log_msg(handler, f"combining attribute: {name}", self.log) for name in names})

    def __setitem__(self, event, handler):
        self.log.log(self.level, f"set handler '{handler}' for vertex attribute '{event}'")
        return super().__setitem__(event, handler)


class EventCombiner:

    @log_init()
    def __init__(self, event_handler, list_handler=None, logger=None, level=DEBUG):
        self.event_handler = event_handler
        self.log = logger or get_logger(self.__class__.__name__)
        self.combine_events = self.make(event_handler, list_handler, self.log)
        if level is not None:
            self.combine_events = log_msg(self.combine_events, "combining events", self.log, level=level)

    def __call__(self, args):
        return self.combine_events(args)

    @staticmethod
    def make(event_handler, list_handler, logger):
        @log_args(logger)
        @wraps(event_handler)
        def _event_combiner(args):
            assert isinstance(args, list)
            if len(args) == 1:
                return args[0]
            # args is list with >= 2 elements
            if are(all, args, events._Event):
                return event_handler(args)
            elif are(all, args, list):
                return list_handler(args) if list_handler else list(chain(*args))
            else:
                logger.error(args)
                raise TypeError(f"unexpected type(s): {set(map(type, args))}")
        return _event_combiner

    def __repr__(self):
        return f"{self.__class__.__name__}({self.event_handler.__module__}.{self.event_handler.__name__})"
