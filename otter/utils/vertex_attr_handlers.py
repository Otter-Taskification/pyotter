from ..logging import get_logger
from . import decorate

module_logger = get_logger(f"{__name__}")

@decorate.log_args(module_logger)
def drop_args(*_args):
    return None

@decorate.log_args(module_logger)
def unique_arg(*_args):
    assert len(_args) == 1 and isinstance(_args[0], list)
    (args,) = _args
    all_identical = all([x is y for x in args for y in args])
    module_logger.debug(f"got {len(args)} args - all items are identical: {all_identical}")
    if all_identical:
        return args[0]
    else:
        raise ValueError("non-unique args")

@decorate.log_args(module_logger)
def unique_or_none(*_args):
    try:
        return unique_arg(*_args)
    except ValueError:
        return None
