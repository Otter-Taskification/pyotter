from typing import Callable, Type
from inspect import isgeneratorfunction, isfunction
from warnings import warn
from functools import partial, wraps


# TODO: prepend f"{func}" to msg to always have function name visible in the warning
def _decorate_function_with_warning(
    func: Callable, msg: str, category: Type[Warning], stacklevel: int
) -> Callable:
    @wraps(func)
    def warning_wrapper(*args, **kwargs):
        warn(msg, category=category, stacklevel=stacklevel)
        return func(*args, **kwargs)

    return warning_wrapper


def _decorate_generator_with_warning(
    func: Callable, msg: str, category: Type[Warning], stacklevel: int
) -> Callable:
    @wraps(func)
    def warning_wrapper(*args, **kwargs):
        warn(msg, category=category, stacklevel=stacklevel)
        yield from func(*args, **kwargs)

    return warning_wrapper


def _with_warning(
    msg: str, category: Type[Warning], stacklevel: int, func: Callable
) -> Callable:
    if isgeneratorfunction(func):
        return _decorate_generator_with_warning(func, msg, category, stacklevel)
    elif callable(func):
        return _decorate_function_with_warning(func, msg, category, stacklevel)
    else:
        raise TypeError(f"expected a function, got {type(func)}")


def with_warning(*arglist, category: Type[Warning] = UserWarning, stacklevel: int = 2):
    """
    Wrap a function to emit a warning each time it is called.

    If called with one argument, return a decorator which will wrap a function with the warning message given as the
    argument.

    If called with two arguments (a message and a function), return the function decorated with the warning message.
    """
    if len(arglist) > 2 or len(arglist) == 0:
        raise TypeError(
            f"{with_warning.__name__} expects 1 or 2 positional arguments, got: {arglist}"
        )
    if len(arglist) == 2:
        msg, fn = arglist
        return _with_warning(msg, category, stacklevel, fn)
    else:  # 1 arg
        (msg,) = arglist
        return partial(_with_warning, msg, category, stacklevel)


# TODO: warning should always include the function name e.g. when called with no args
def warn_deprecated(*arglist, stacklevel: int = 2):
    """
    Wrap a function with a warning that it is deprecated. If passed a string, return a decorator which will wrap a
    function to issue a warning containing that string
    """
    argc = len(arglist)
    if argc not in [0, 1]:
        raise TypeError(
            f"{warn_deprecated.__name__} expected at most 1 positional argument, got: {arglist}"
        )
    if argc == 1:
        (arg,) = arglist
        if isinstance(arg, str):
            return partial(
                with_warning, arg, category=DeprecationWarning, stacklevel=stacklevel
            )
        elif isfunction(arg):
            return with_warning(
                f"{arg} is deprecated",
                arg,
                category=DeprecationWarning,
                stacklevel=stacklevel,
            )
        else:
            raise TypeError(f"dont know how to decorate {type(arg)}")
    else:
        return partial(
            with_warning,
            "function is deprecated",
            category=DeprecationWarning,
            stacklevel=stacklevel,
        )
