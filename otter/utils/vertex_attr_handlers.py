from ..logging import get_logger
from ..definitions import RegionType
from ..EventFactory import events
from . import decorate

module_logger = get_logger(f"{__name__}")

def are(reduce, args, T):
    return reduce(isinstance(x, T) for x in args)

@decorate.log_args(module_logger)
def drop_args(args):
    return None

@decorate.log_args(module_logger)
def unique_arg(args):
    assert isinstance(args, list)
    all_identical = all([x is y for x in args for y in args])
    module_logger.debug(f"got {len(args)} args - all items are identical: {all_identical}")
    if all_identical:
        return args[0]
    else:
        module_logger.debug("DUMPING ARGS:")
        for n, arg in enumerate(args):
            module_logger.debug(f"{n:>2}: {arg}")
        raise ValueError("non-unique args")

@decorate.log_args(module_logger)
def unique_or_none(*_args):
    try:
        return unique_arg(*_args)
    except ValueError:
        return None

@decorate.log_args(module_logger)
def pass_single_executor(args):
    assert isinstance(args, list)
    assert are(all, args, events._Event)
    if len(args) == 1:
        raise RuntimeError(f"shouldn't pass list of 1 event: {args}")
    # args is a list of >1 events._Event
    single_executor_events = set(e for e in args if e.region_type == RegionType.single_executor)
    if any(single_executor_events):
        event = single_executor_events.pop()
        module_logger.debug(f"returning single event: {event}")
        return event
    else:
        module_logger.debug(f"{set(e.region_type for e in args)}")
        module_logger.debug(f"returning event list")
        return args

@decorate.log_args(module_logger)
def pass_master_event(args):
    region_types = {e.region_type for e in args}
    if region_types == {'master'} and len(set(args)) == 1:
        return args[0]
    else:
        return args

def make_event_combiner(event_handler, list_handler=None):
    @decorate.log_args(module_logger)
    def event_combiner(args):
        assert isinstance(args, list)
        if len(args) == 1:
            return args[0]
        # args is list with >= 2 elements
        if are(all, args, events._Event):
            return event_handler(args)
        elif are(all, args, list):
            return list_handler(args) if list_handler else list(chain(*args))
        else:
            module_logger.error(args)
            raise TypeError(f"unexpected type(s): {set(map(type, args))}")
    event_combiner.__module__ = event_handler.__module__
    event_combiner.__name__ = event_handler.__name__
    return event_combiner
