from __future__ import annotations
import os
import abc
import sqlite3
import address_to_line as a2l
from typing import AnyStr, Set, Dict, Iterable, Tuple
from otf2.definitions import Attribute as OTF2Attribute
from otf2 import LocationType as OTF2Location
from . import log
from . import utils
from .reader import get_otf2_reader
from .core.event_model.event_model import EventModel, get_event_model
from .core.events import Event, Location
from .core.tasks import TaskRegistry, Task


class Project(abc.ABC):

    def __init__(self, anchorfile: AnyStr, debug: bool = False) -> None:
        self.debug = debug
        self.log = log.get_logger("project")
        self.project_root = os.path.dirname(anchorfile)
        self.anchorfile = anchorfile
        self.aux_dir = "aux"
        self.debug_dir = "debug_output"
        self.maps_file = self.abspath(os.path.join(self.aux_dir, "maps"))
        self.source_location_db = self.abspath(os.path.join(self.aux_dir, "srcloc.db"))
        self.return_addresses: Set[int] = set()
        self.event_model = None
        self.task_registry = TaskRegistry()
        self.chunks = list()
        if self.debug:
            if not os.path.exists(self.abspath(self.debug_dir)):
                os.mkdir(self.abspath(self.debug_dir))
        self._check_input_files()

    def _check_input_files(self) -> None:
        if not os.path.isdir(self.abspath(self.aux_dir)):
            raise NotADirectoryError(self.abspath(self.aux_dir))
        if not os.path.isfile(self.maps_file):
            raise FileNotFoundError(self.maps_file)
        self.log.info(f"Found maps file: {self.maps_file}")

    def abspath(self, relname: AnyStr) -> AnyStr:
        return os.path.abspath(os.path.join(self.project_root, relname))

    def process_trace(self) -> Project:
        with get_otf2_reader(self.anchorfile) as reader:
            event_model_name: EventModel = reader.get_event_model_name()
            self.event_model = get_event_model(event_model_name, self.task_registry, gather_return_addresses=self.return_addresses)
            self.log.info(f"Found event model name: {str(event_model_name)}")
            self.log.info(f"Using event model: {self.event_model}")
            self.log.info(f"generating chunks")
            attributes: Dict[str: OTF2Attribute] = {attr.name: attr for attr in reader.definitions.attributes}
            locations: Dict[OTF2Location: Location] = {location: Location(location) for location in
                                                       reader.definitions.locations}
            event_iter: Iterable[Tuple[Location, Event]] = ((locations[location], Event(event, attributes)) for
                                                            location, event in reader.events)
            self.chunks = list(self.event_model.yield_chunks(event_iter))
            # TODO: temporary check, factor out once new event models are passing
            self.event_model.warn_for_incomplete_chunks(self.chunks)
            self.graphs = list(self.event_model.chunk_to_graph(chunk) for chunk in self.chunks)
            self.graph = self.event_model.combine_graphs(self.graphs)
        return self

    def resolve_return_addresses(self) -> Project:
        self.log.info(f"writing source location information to {self.source_location_db}")
        source_locations = a2l.resolve_source_locations(self.maps_file, list(self.return_addresses))
        if self.debug:
            for location in source_locations:
                self.log.debug(f"{location}")
        with sqlite3.connect(self.source_location_db) as con:
            data = ((f"0x{s.address:0>16x}", s.function, s.file, s.line) for s in source_locations)
            con.executescript("""
                    drop table if exists files;
                    drop table if exists functions;
                    drop table if exists src_loc;

                    create table files(id, path);
                    create table functions(id, name);
                    create table src_loc(address, function, file, line);
                    """)
            con.executemany("insert into src_loc values(?, ?, ?, ?)", data)
        return self

    def unpack_vertex_event_attributes(self) -> None:
        for vertex in self.graph.vs:
            event_list = vertex['event_list']
            self.log.debug(f"unpacking vertex event_list:")
            if self.debug:
                for event in event_list:
                    self.log.debug(f"  {event}")
            attributes = self.event_model.unpack(event_list)
            for key, value in attributes.items():
                self.log.debug(f"  got {key}={value}")
                if isinstance(value, list):
                    s = set(value)
                    if len(s) == 1:
                        value = s.pop()
                    else:
                        self.log.debug(f"  concatenate {len(value)} values")
                        value = ";".join(str(item) for item in value)
                if isinstance(value, int):
                    value = str(value)
                elif value == "":
                    value = None
                self.log.debug(f"    unpacked {value=}")
                vertex[key] = value

    def cleanup_temporary_attributes(self) -> None:
        for name in self.graph.vs.attribute_names():
            if name.startswith("_"):
                del self.graph.vs[name]


    @abc.abstractmethod
    def run(self) -> Project:
        self.process_trace()
        self.resolve_return_addresses()
        if self.debug:
            utils.assert_vertex_event_list(self.graph)
            self.log.info(f"dumping chunks, tasks and graphs to log files")
            utils.dump_to_log_file(self.chunks, self.graphs, self.task_registry, where=self.abspath(self.debug_dir))
            graph_log = self.abspath(os.path.join(self.debug_dir, "graph.log"))
            utils.dump_graph_to_file(self.graph, filename=graph_log, no_flatten=[Task,])
        self.unpack_vertex_event_attributes()
        self.cleanup_temporary_attributes()
        del self.graph.vs['event_list']
        return self


class SimpleProject(Project):
    """Transforms an OTF2 trace into a basic HTML report"""

    def run(self) -> SimpleProject:
        super().run()
        return self
