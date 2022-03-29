
# pure decorator
def log_init(init_func):
    def init_wrapper(self, *args, **kwargs):
        init_func(self, *args, **kwargs)
        if hasattr(self, "log"):
            self.log.debug(f"INITIALISED: {self.__class__.__name__}({args=}, {kwargs=})")
    return init_wrapper

# returns a decorator which decorates func by logging the args passed
def log_args(logger):
    def decorator(func):
        def wrapper(*args, **kwargs):
            logger.debug(f"calling {func.__module__}.{func.__name__} with {len(args)} arg(s) and {len(kwargs)} kwarg(s):")
            for n, arg in enumerate(args):
                logger.debug(f"{n:>2}: {arg}")
            for m, (key, item) in enumerate(kwargs.items()):
                logger.debug(f"{m:>2}: {key}={item}")
            return func(*args, **kwargs)
        wrapper.__module__ = func.__module__
        wrapper.__name__ = func.__name__
        return wrapper
    return decorator

# returns a decorator which sends debug message msg to logger before calling func
def _log_call_with_msg(msg, logger):
    def decorator(func):
        def wrapper(*args, **kwargs):
            logger.debug(f"calling {func.__module__}.{func.__name__}: {msg}")
            return func(*args, **kwargs)
        return wrapper
    return decorator

# returns a decorated version of func which sends debug message msg to logger
def log_call_with_msg(func, msg, logger):
    decorator = _log_call_with_msg(msg, logger)
    return decorator(func)
