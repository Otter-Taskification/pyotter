try:
    from importlib import metadata
except ImportError:
    import importlib_metadata as metadata

__version__ = metadata.version("otter")

from . import args, core, log, main, project, reader, reporting, styling, utils

utils.find_dot_or_die()
