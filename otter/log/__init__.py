import logging
from logging import DEBUG, INFO, WARN, ERROR
from loggingdecorators import on_call


def dict_lines(d, spaces=0):
    prefix = "  " * spaces
    for k in d:
        if isinstance(d[k], dict):
            yield f"{prefix}{k}:"
            yield from dict_lines(d[k], spaces=spaces + 1)
        else:
            yield f"{prefix}{k}: {d[k]}"


def logger_lines(log: logging.getLoggerClass()):
    yield f"  {log} (level={log.level}, handlers={len(log.handlers)}, propagate={log.propagate})"
    if len(log.handlers) > 0:
        for handler in log.handlers:
            yield f"    {handler=}"


class Logging:

    def __init__(self):
        raise NotImplementedError()

    _root = None
    _init_logger = None
    _initialise = None

    @classmethod
    def init(cls, args):
        import logging
        import os
        from logging import config as logging_config
        import yaml
        from itertools import chain
        from . import config

        try:
            import importlib.resources as pkg_resources
        except ImportError:
            # Try backported to PY<37 `importlib_resources`.
            import importlib_resources as pkg_resources

        conf_text = pkg_resources.read_text(config, "standard.yaml")
        conf = yaml.safe_load(conf_text)

        conf['root']['level'] = args.loglevel

        for hconf in conf['handlers'].values():
            if 'filename' in hconf:
                hconf['filename'] = os.path.join(args.logdir, hconf['filename'])

        logging_config.dictConfig(conf)

        cls._root = logging.getLogger('otter')
        cls._init_logger = logging.getLogger(conf['otter']['init'])
        cls._initialise = logging.getLogger(conf['otter']['initialise'])

        logger = cls._initialise

        logger.info(f"logging was initialised:")
        logger.info(f"  {args.loglevel=}")
        logger.info(f"  {args.logdir=}")

        logger.debug(">>> BEGIN LOGGING CONFIGURATION <<<")
        for line in dict_lines(conf):
            logger.debug(line)
        logger.debug(">>> END LOGGING CONFIGURATION <<<")

        logger.debug(">>> BEGIN LOGGERS <<<")
        logger.debug("loggers:")
        for log in chain([logging.getLogger()], map(logging.getLogger, conf['loggers'])):
            for line in logger_lines(log):
                logger.debug(line)
        logger.debug(">>> END LOGGERS <<<")


def get_logger(name: str):
    """
    Provides a wrapper around logging.getLogger to ensure that all loggers not defined in the config file are children
    of the "otter" logger.

    If 'name' is an attribute of the Logging class, return this attribute if it is a logger. Otherwise, get a reference
    to a logger of this name which is a child of the "otter" logger. If this is not a logger defined in the standard
    config file, it will propagate its messages up to the parent "otter" logger.
    """
    log = Logging._initialise
    if hasattr(Logging, f"_{name}"):
        logger = getattr(Logging, f"_{name}")
        if isinstance(logger, logging.getLoggerClass()):
            result = logger
        elif logger is None:
            result = logging.getLogger()
        else:
            raise TypeError(f"unknown type: {type(logger)=}")
    else:
        try:
            result = logging.getLogger(f"{Logging._root.name}.{name}")
        except AttributeError:
            result = logging.getLogger()
    if log is not None:
        log.debug(f"got logger for {name=}:", stacklevel=2)
        for line in logger_lines(result):
            log.debug(line, stacklevel=2)
    return result
