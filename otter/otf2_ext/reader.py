"""Extensions to otf2"""

from typing import Iterable, Dict, Tuple, Any

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
        self._evt_reader_handles: Dict[int, Any] = {
            location._ref: _otf2.Reader_GetEvtReader(self._handle, location._ref)
            for location in self._locations
        }

    @property
    def properties(self) -> Dict[str, str]:
        return self._properties

    def get_property(self, prop_name: str) -> str:
        return self._properties[prop_name]
    
    def seek_events(self, positions: Iterable[Tuple[int, int]]):
        reader = SeekingEventReader(self.definitions)
        for location, event in reader.seek_events(positions, self._evt_reader_handles):
            yield location, event
