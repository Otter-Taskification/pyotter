import logging
import loggingdecorators as logdec
from logging import DEBUG, INFO, WARN, ERROR


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
    import yaml
    from logging import config as logging_config
    from itertools import chain
    from . import config

    try:
        import importlib.resources as resources
    except ImportError:
        import importlib_resources as resources

    conf_text = resources.read_text(config, "standard.yaml")
    conf = yaml.safe_load(conf_text)

    conf['root']['level'] = args.loglevel

    for hconf in conf['handlers'].values():
        if 'filename' in hconf:
            hconf['filename'] = os.path.join(args.logdir, hconf['filename'])

    logging_config.dictConfig(conf)

    root_logger = logging.getLogger('otter')
    init_logger = logging.getLogger(conf['otter']['init'])
    initialise_logger = logging.getLogger(conf['otter']['initialise'])

    _state_.set_logger("_root", root_logger)
    _state_.set_logger("_init_logger", init_logger)
    _state_.set_logger("_initialise", initialise_logger)

    initialise_logger.debug(">>> BEGIN LOGGING CONFIGURATION <<<")
    for line in dict_lines(conf):
        initialise_logger.debug(line)
    initialise_logger.debug(">>> END LOGGING CONFIGURATION <<<")

    initialise_logger.debug(">>> BEGIN LOGGERS <<<")
    initialise_logger.debug("loggers:")
    for log in chain([logging.getLogger()], map(logging.getLogger, conf['loggers'])):
        for line in logger_lines(log):
            initialise_logger.debug(line)
    initialise_logger.debug(">>> END LOGGERS <<<")

    _state_.initialise()

    initialise_logger.info(f"logging was initialised:")
    initialise_logger.info(f"  {args.loglevel=}")
    initialise_logger.info(f"  {args.logdir=}")


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
    Provides a wrapper around logging.getLogger to ensure that all loggers not defined in the config file are children
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
        logger = logging.getLogger(f"{root_name}.{name}")
    finally:
        log.debug(f"got logger for {name=}:", stacklevel=2)
        for line in logger_lines(logger):
            log.debug(line, stacklevel=2)

    return logger


def logger_lines(log: logging.getLoggerClass()):
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
