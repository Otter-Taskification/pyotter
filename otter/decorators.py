from functools import wraps
from logging import DEBUG, INFO


def log_init(level=INFO):
    """decorator factory: create a post-init decorator which logs an __init__ call with the specified level"""
    def decorator(init_func):
        @wraps(init_func)
        def init_wrapper(self, *args, **kwargs):
            init_func(self, *args, **kwargs)
            if hasattr(self, "log") and hasattr(self.log, "log"):
                self.log.log(level, f"INITIALISED: {self.__class__.__name__}({args=}, {kwargs=})")
            else:
                raise AttributeError(f"Can't log initialisation of {self.__class__.__name__} - missing log attribute")
        return init_wrapper
    return decorator


def log_args(logger, level=DEBUG):
    """decorator factory: return a decorator which logs the args passed to the wrapped function func"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger.log(level, f"calling {func.__module__}.{func.__name__} with {len(args)} arg(s) and {len(kwargs)} kwarg(s):")
            for n, arg in enumerate(args):
                logger.log(level, f"{n:>2} {type(arg)}: {arg}")
            for m, (key, item) in enumerate(kwargs.items()):
                logger.log(level, f"{m:>2}: {key}={type(arg)} {item}")
            return func(*args, **kwargs)
        return wrapper
    return decorator


def _log_msg(msg, logger, level):
    """decorator factory: return a decorator which logs a message before calling the wrapped function func"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger.log(level, f"CALLING {func}: {msg}")
            return func(*args, **kwargs)
        return wrapper
    return decorator


def log_msg(func, msg, logger, level=DEBUG):
    """decorate func with a message in the log at the given level each time it is called"""
    decorator = _log_msg(msg, logger, level)
    return decorator(func)
