from contextlib import closing
from otf2.reader import Reader

try:
    from _otf2 import Reader_GetPropertyNames, Reader_GetProperty
except ImportError as err:

    def Reader_GetPropertyNames(*_):
        return list()
    
    def Reader_GetProperty(self, property_name):
        raise AttributeError(f"property '{property_name}' not defined")


class _OTF2Reader(Reader):
    """Extends the otf2 reader with access to trace properties"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def get_properties(self):
        return {name: Reader_GetProperty(self.handle, name) for name in Reader_GetPropertyNames(self.handle)}


def get_otf2_reader(*args, **kwargs):
    # allows _OTF2Reader to be used in a with-block
    return closing(_OTF2Reader(*args, **kwargs))
