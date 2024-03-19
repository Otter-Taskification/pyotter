from __future__ import annotations

from enum import Enum
from logging import DEBUG, ERROR, INFO, WARN, Logger, getLogger

import colorama as _color

_log: Logger = Logger("_dontuse_", level=99)
_is_initialised: bool = False


class Level(int, Enum):
    """Define debugging levels"""

    DEBUG = DEBUG
    INFO = INFO
    WARN = WARN
    ERROR = ERROR


def as_level(name: str) -> Level:
    """Get the level corresponding to the name of a logging level"""
    return getattr(Level, name.upper(), Level.WARN)


def initialise(level: str = "warn"):

    import importlib.resources as resources
    from logging import config as logging_config
    import yaml
    from . import config

    conf_text = resources.read_text(config, "standard.yaml")
    conf = yaml.safe_load(conf_text)

    conf["root"]["level"] = as_level(level)

    logging_config.dictConfig(conf)

    global _log, _is_initialised
    _log = getLogger("main")
    _is_initialised = True

    info("logging initialised (level=%s)", level)

    debug("logging configuration:")
    for line in dict_lines(conf):
        debug(line)
    debug("")

    debug("loggers:")
    for log in (getLogger(), *map(getLogger, conf["loggers"])):
        for line in logger_lines(log):
            debug(line)
    debug("")


def is_initialised():
    return is_initialised


def logger_lines(log):
    yield f"  {log} (level={log.level}, handlers={len(log.handlers)}, propagate={log.propagate})"
    if len(log.handlers) > 0:
        for handler in log.handlers:
            yield f"    {handler=}"


def dict_lines(d, spaces=0):
    prefix = "  " * spaces
    for k in d:
        if isinstance(d[k], dict):
            yield f"{prefix}{k}:"
            yield from dict_lines(d[k], spaces=spaces + 1)
        else:
            yield f"{prefix}{k}: {d[k]}"


def error(msg, *args):
    _log.error(_color.Fore.RED + msg + _color.Style.RESET_ALL, *args)


def warning(msg, *args):
    _log.warning(_color.Fore.YELLOW + msg + _color.Style.RESET_ALL, *args)


def info(msg, *args):
    _log.info(msg, *args)


def debug(msg, *args):
    _log.debug(_color.Fore.CYAN + msg + _color.Style.RESET_ALL, *args)


def is_enabled(level):
    return _log.isEnabledFor(level)


def is_debug_enabled():
    "Check whether logging is enabled at this level"
    return _log.isEnabledFor(DEBUG)


def is_info_enabled():
    "Check whether logging is enabled at this level"
    return _log.isEnabledFor(INFO)


def is_warn_enabled():
    "Check whether logging is enabled at this level"
    return _log.isEnabledFor(WARN)


def is_error_enabled():
    "Check whether logging is enabled at this level"
    return _log.isEnabledFor(ERROR)


def log_with_prefix(prefix, logging_func, sep=" "):
    def wrapper(msg, *args):
        logging_func(prefix + sep + msg, *args)

    return wrapper
