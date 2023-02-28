from contextlib import closing
from typing import Dict, Any, Union
from otf2.reader import Reader
from otter.definitions import TraceAttr, EventModel

try:
    from _otf2 import Reader_GetPropertyNames, Reader_GetProperty
except ImportError as err:

    def Reader_GetPropertyNames(*_):
        return list()
    
    def Reader_GetProperty(self, property_name: str) -> str:
        raise AttributeError(f"property '{property_name}' not defined")


class _OTF2Reader(Reader):
    """Extends the otf2 reader with access to trace properties"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._properties: Dict[str, str] = {name: Reader_GetProperty(self.handle, name) for name in Reader_GetPropertyNames(self.handle)}

    @property
    def properties(self) -> Dict[Any, Any]:
        return self._properties

    def get_property(self, prop_name: str) -> str:
        return self._properties[prop_name]

    def get_event_model_name(self) -> EventModel:
        return EventModel(self.get_property(TraceAttr.event_model))


def get_otf2_reader(*args, **kwargs) -> closing[_OTF2Reader]:
    # allows _OTF2Reader to be used in a with-block
    return closing(_OTF2Reader(*args, **kwargs))
