from __future__ import annotations

from enum import Enum
from logging import DEBUG, ERROR, INFO, WARN, Logger, getLogger
from typing import Optional

import colorama as _color


class Level(int, Enum):
    """Define debugging levels"""

    DEBUG = DEBUG
    INFO = INFO
    WARN = WARN
    ERROR = ERROR


def as_level(name: str) -> Level:
    """Get the level corresponding to the name of a logging level"""
    return getattr(Level, name.upper())


class _state_:
    _is_initialised = False
    _root = None
    _init_logger = None
    _initialise = None

    def __init__(self):
        raise NotImplementedError("This class should not be instantiated")

    @classmethod
    def initialise(cls):
        cls._is_initialised = True

    @classmethod
    def set_logger(cls, name, logger):
        setattr(cls, name, logger)

    @classmethod
    def get_logger(cls, name):
        return getattr(cls, name)

    @classmethod
    def is_initialised(cls):
        return cls._is_initialised


def initialise(args):
    import os
    from logging import config as logging_config

    import yaml

    from . import config

    try:
        import importlib.resources as resources
    except ImportError:
        import importlib_resources as resources

    if not os.path.isdir(args.logdir):
        os.mkdir(args.logdir)

    conf_text = resources.read_text(config, "standard.yaml")
    conf = yaml.safe_load(conf_text)

    conf["root"]["level"] = as_level(args.loglevel)

    for hconf in conf["handlers"].values():
        if "filename" in hconf:
            hconf["filename"] = os.path.join(args.logdir, hconf["filename"])

    logging_config.dictConfig(conf)

    root_logger = getLogger("otter")
    init_logger = getLogger(conf["otter"]["init"])
    initialise_logger = getLogger(conf["otter"]["initialise"])

    _state_.set_logger("_root", root_logger)
    _state_.set_logger("_init_logger", init_logger)
    _state_.set_logger("_initialise", initialise_logger)

    initialise_logger.debug(">>> BEGIN LOGGING CONFIGURATION <<<")
    for line in dict_lines(conf):
        initialise_logger.debug(line)
    initialise_logger.debug(">>> END LOGGING CONFIGURATION <<<")

    initialise_logger.debug(">>> BEGIN LOGGERS <<<")
    initialise_logger.debug("loggers:")
    for log in (getLogger(), *map(getLogger, conf["loggers"])):
        for line in logger_lines(log):
            initialise_logger.debug(line)
    initialise_logger.debug(">>> END LOGGERS <<<")

    _state_.initialise()

    initialise_logger.info("logging was initialised:")
    initialise_logger.info("  args.loglevel=%s", args.loglevel)
    initialise_logger.info("  args.logdir=%s", args.logdir)


def is_initialised():
    return _state_.is_initialised()


def logger_getter(name):
    def getter():
        if not is_initialised():
            raise RuntimeError("logging is not initialised")
        return get_logger(name)

    return getter


def get_logger(name: str):
    """
    Provides a wrapper around getLogger to ensure that all loggers not defined in the config file are children
    of the "otter" logger.

    If 'name' is an attribute of the Logging class, return this attribute if it is a logger. Otherwise, get a reference
    to a logger of this name which is a child of the "otter" logger. If this is not a logger defined in the standard
    config file, it will propagate its messages up to the parent "otter" logger.
    """

    if not is_initialised():
        raise RuntimeError("Logging not initialised")

    log = _state_.get_logger("_initialise")

    try:
        logger = _state_.get_logger(name)
    except AttributeError:
        root_name = _state_.get_logger("_root").name
        logger = getLogger(f"{root_name}.{name}")
    finally:
        log.debug(f"got logger for {name=}:", stacklevel=2)
        for line in logger_lines(logger):
            log.debug(line, stacklevel=2)

    return logger


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
    log = get_logger("main")
    log.error(_color.Fore.RED + msg + _color.Style.RESET_ALL, *args)


def warning(msg, *args):
    log = get_logger("main")
    log.warning(_color.Fore.YELLOW + msg + _color.Style.RESET_ALL, *args)


def info(msg, *args):
    log = get_logger("main")
    log.info(msg, *args)


def debug(msg, *args):
    log = get_logger("main")
    log.debug(_color.Fore.CYAN + msg + _color.Style.RESET_ALL, *args)


def is_enabled(level):
    return get_logger("main").isEnabledFor(level)


def is_debug_enabled(logger: Optional[Logger] = None):
    "Check whether logging is enabled at this level"

    logger = logger or get_logger("main")
    return logger.isEnabledFor(DEBUG)


def is_info_enabled(logger: Optional[Logger] = None):
    "Check whether logging is enabled at this level"

    logger = logger or get_logger("main")
    return logger.isEnabledFor(INFO)


def is_warn_enabled(logger: Optional[Logger] = None):
    "Check whether logging is enabled at this level"

    logger = logger or get_logger("main")
    return logger.isEnabledFor(WARN)


def is_error_enabled(logger: Optional[Logger] = None):
    "Check whether logging is enabled at this level"

    logger = logger or get_logger("main")
    return logger.isEnabledFor(ERROR)
