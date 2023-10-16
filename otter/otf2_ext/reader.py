"""Extensions to otf2"""

from typing import Iterable, Dict, Tuple, Any

from otf2.definitions import Attribute as OTF2Attribute
from otf2.reader import Reader
import _otf2

from .event_reader import SeekingEventReader


class OTF2Reader(Reader):
    """Extends the otf2 reader with access to trace properties and event seeking"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._properties: Dict[str, str] = {
            str(name): str(_otf2.Reader_GetProperty(self.handle, name))
            for name in _otf2.Reader_GetPropertyNames(self.handle)
        }
        self._attribute_dict: Dict[str, OTF2Attribute] = {
            attr.name: attr for attr in self.definitions.attributes
        }

    @property
    def properties(self) -> Dict[str, str]:
        return self._properties

    @property
    def attributes(self) -> Dict[str, OTF2Attribute]:
        return self._attribute_dict

    def get_property(self, prop_name: str) -> str:
        return self._properties[prop_name]

    def seek_events(self, positions: Iterable[Tuple[int, int]]):
        reader = SeekingEventReader(self.definitions)
        # create the evt_readers fresh each time as it appears that the handles
        # can go stale if created during __init__ - have observed evt_readers
        # created during __init__ being free'd internally within OTF2, meaning
        # the python handle becomes unknowingly invalid
        evt_readers: Dict[int, Any] = {
            location._ref: _otf2.Reader_GetEvtReader(self._handle, location._ref)
            for location in self._locations
        }
        for location, event in reader.seek_events(positions, evt_readers):
            yield location, event
        # clean up the evt readers each time we seek to ensure any internal
        # handles are removed
        for evt_reader in evt_readers.values():
            _otf2.Reader_CloseEvtReader(self._handle, evt_reader)

    def get_evt_reader_pos(self, location_ref: int):
        evt_reader = _otf2.Reader_GetEvtReader(self._handle, location_ref)
        return _otf2.EvtReader_GetPos(evt_reader)
