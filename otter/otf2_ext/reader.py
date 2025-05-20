"""Extensions to otf2 Reader"""

from contextlib import contextmanager, closing
from typing import Dict, Any

import _otf2
from otf2.definitions import Attribute, Location
from otf2.registry import DefinitionRegistry

from .event_reader import GlobalEventReader, SeekingEventReader


class Reader:
    """Opens OTF2 traces and manages the OTF2 Reader handle"""

    def __init__(self, anchor_file) -> None:
        super().__init__()
        self._anchor_file = anchor_file
        self._definitions = DefinitionRegistry()

        # the OTF2_Reader handle
        self._reader_handle = _otf2.Reader_Open(anchor_file)
        _otf2.Reader_SetSerialCollectiveCallbacks(self._reader_handle)

        major, minor, _ = _otf2.Reader_GetVersion(self._reader_handle)
        self.trace_version = (major, minor)

        # read the global definitions
        global_def_reader = _otf2.Reader_GetGlobalDefReader(self._reader_handle)
        def_reader_callbacks = _otf2.GlobalDefReaderCallbacks_New()
        self._definitions._set_global_def_reader_callbacks(def_reader_callbacks)
        _otf2.Reader_RegisterGlobalDefCallbacks(self._reader_handle, global_def_reader, def_reader_callbacks, self._definitions)
        _otf2.GlobalDefReaderCallbacks_Delete(def_reader_callbacks)
        _otf2.Reader_ReadAllGlobalDefinitions(self._reader_handle, global_def_reader)
        _otf2.Reader_CloseGlobalDefReader(self._reader_handle, global_def_reader)

        # map location ref to location instance
        self._locations: Dict[int, Location] = {}
        for location in self._definitions.locations:
            self._locations[location._ref] = location

        # map defined properties
        self._properties: Dict[str, str] = {}
        for name in self.get_property_names():
            self._properties[str(name)] = self.get_property(name)

        # map attribute names to the attribute instance
        self._attributes: Dict[str, Attribute] = {}
        for attr in self._definitions.attributes:
            self._attributes[attr.name] = attr

    def close(self):
        if self._reader_handle is not None:
            _otf2.Reader_Close(self._reader_handle)
            self._reader_handle = None

    @property
    def handle(self):
        if self._reader_handle is None:
            raise ValueError("invalid reader handle")
        return self._reader_handle

    @property
    def definitions(self):
        return self._definitions
    
    @property
    def attributes(self):
        return self._attributes
    
    @property
    def locations(self):
        return self._locations

    @property
    def properties(self):
        return self._properties
    
    def get_property_names(self):
        return [str(item) for item in _otf2.Reader_GetPropertyNames(self._reader_handle)]

    def get_property(self, name: str):
        return str(_otf2.Reader_GetProperty(self._reader_handle, name))
    
    @contextmanager
    def events(self):
        for ref in self.locations:
            _otf2.Reader_SelectLocation(self.handle, ref)
            _otf2.Reader_GetEvtReader(self.handle, ref)
        handle = _otf2.Reader_GetGlobalEvtReader(self.handle)
        try:
            yield GlobalEventReader(handle, self.definitions)
        finally:
            _otf2.Reader_CloseGlobalEvtReader(self.handle, handle)

    @contextmanager
    def seek_events(self):
        handles: Dict[int, Any] = {}
        for ref in self.locations:
            _otf2.Reader_SelectLocation(self.handle, ref)
            handles[ref] = _otf2.Reader_GetEvtReader(self.handle, ref)
        try:
            seeking_reader = SeekingEventReader(handles, self.definitions)
            yield seeking_reader.events
        finally:
            for handle in handles.values():
                _otf2.Reader_CloseEvtReader(self.handle, handle)

@contextmanager
def open_trace(anchor_file):
    with closing(Reader(anchor_file)) as reader:
        yield reader
