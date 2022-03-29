import logging

module_logger = logging.getLogger(f'{__name__}')

def initialise(args):
    _ = logging.getLogger()
    level = getattr(logging, args.log.upper())
    _.setLevel(level)

    # create console handler
    ch = logging.StreamHandler()
    ch.setLevel(level)

    # create file handler
    fh = logging.FileHandler('otter.log', mode='w')
    fh.setLevel(level)

    # create formatter and apply to handlers
    f = logging.Formatter(
        fmt='[{asctime}] {filename:<20}(#{lineno:>03}) [{levelname}] {message}',
        datefmt='%Y/%m/%d-%H:%m:%S',
        style="{"
    )
    ch.setFormatter(f)
    fh.setFormatter(f)

    # Attach handlers
    _.addHandler(ch)
    _.addHandler(fh)

def get_logger(name: str = None) -> logging.Logger:
    # module_logger.debug(f"getting logger '{name}'")
    log = logging.getLogger(name=name)
    return log
