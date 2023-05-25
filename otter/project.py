from __future__ import annotations
import os
import abc
import sqlite3
import address_to_line as a2l
from contextlib import closing
from collections import defaultdict
from itertools import count
from typing import AnyStr, Set, Dict, Iterable, Tuple, Set
from otf2.definitions import Attribute as OTF2Attribute
from otf2 import LocationType as OTF2Location
from . import log
from . import utils
from . import db
from .reader import get_otf2_reader
from .definitions import SourceLocation
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
        self.tasks_db = self.abspath(os.path.join(self.aux_dir, "tasks.db"))
        self.return_addresses: Set[int] = set()
        self.event_model = None
        self.task_registry = TaskRegistry()
        self.chunks = list()
        self._check_input_files()
        self._prepare_environment()

    def _check_input_files(self) -> None:
        if not os.path.isdir(self.abspath(self.aux_dir)):
            raise NotADirectoryError(self.abspath(self.aux_dir))
        if not os.path.isfile(self.maps_file):
            raise FileNotFoundError(self.maps_file)
        self.log.info(f"Found maps file: {self.maps_file}")

    def _prepare_environment(self) -> None:
        """Prepare the environment - create any folders, databases, etc required by the project"""
        abs_debug_dir = self.abspath(self.debug_dir)
        self.log.info(f"preparing environment")
        if self.debug:
            if not os.path.exists(abs_debug_dir):
                os.mkdir(abs_debug_dir)
        if os.path.exists(self.tasks_db):
            self.log.warn(f"overwriting tasks database {self.tasks_db}")
            os.remove(self.tasks_db)
        else:
            self.log.info(f"creating tasks database {self.tasks_db}")
        with closing(sqlite3.connect(self.tasks_db)) as con:
            con.executescript(db.scripts.create_tasks)
            con.executescript(db.scripts.create_views)

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
            # self.event_model.warn_for_incomplete_chunks(self.chunks)
            # self.graphs = list(self.event_model.chunk_to_graph(chunk) for chunk in self.chunks)
            # self.graph = self.event_model.combine_graphs(self.graphs)

        if self.debug:
            self.log.debug(f"task registry encountered the following task attributes:")
            for name in sorted(self.task_registry.attributes):
                self.log.debug(f" -- {name}")

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
        self.log.info("unpacking vertex event attributes")
        logger = log.get_logger("unpack_event")
        for vertex in self.graph.vs:
            event_list = vertex['event_list']
            logger.debug(f"unpacking vertex event_list")
            if self.debug:
                for event in event_list:
                    logger.debug(f"  {event}")
            attributes = self.event_model.unpack(event_list)
            for key, value in attributes.items():
                logger.debug(f"  got {key}={value}")
                if isinstance(value, list):
                    s = set(value)
                    if len(s) == 1:
                        value = s.pop()
                    else:
                        logger.debug(f"  concatenate {len(value)} values")
                        value = ";".join(str(item) for item in value)
                if isinstance(value, int):
                    value = str(value)
                elif value == "":
                    value = None
                logger.debug(f"    unpacked {value=}")
                vertex[key] = value

    def cleanup_temporary_attributes(self) -> None:
        self.log.info("cleaning up temporary graph attributes")
        for name in self.graph.vs.attribute_names():
            if name.startswith("_"):
                del self.graph.vs[name]

    def write_tasks_to_db(self) -> None:
        self.log.info(f"Writing tasks to {self.tasks_db}")

        with closing(sqlite3.connect(self.tasks_db)) as con:

            # Create unique labels for distinct source locations
            source_location_counter = count()
            string_counter = count()
            source_location_id: Dict[SourceLocation, int] = defaultdict(lambda: next(source_location_counter))
            string_id: Dict[str, int] = defaultdict(lambda: next(string_counter))

            # Write the tasks and their source locations
            for tasks in utils.batched(self.task_registry, 1000):

                # TODO: insert task source locations into main task table

                task_data = ((
                    task.id,
                    str(task.start_ts),
                    str(task.end_ts),
                    source_location_id[task.initialised_at],
                    source_location_id[task.started_at],
                    source_location_id[task.ended_at],
                    task.flavour,
                    string_id[task.user_label]
                ) for task in tasks)
                con.executemany(db.scripts.insert_tasks, task_data)

            con.commit()

            # Write the definitions of the source locations, mapping their IDs to string IDs
            source_location_definitions = ((locid, string_id[location.file], string_id[location.func], location.line) for (location, locid) in source_location_id.items())
            con.executemany(db.scripts.define_source_locations, source_location_definitions)

            # Write the string definitions mapping ID to the string value
            string_definitions = ((string_key, string) for (string, string_key) in string_id.items())
            con.executemany(db.scripts.define_strings, string_definitions)

            # Write the task parent-child relationships
            all_relations = ((task.id, child_id) for task in self.task_registry for child_id in task.children)
            for relations in utils.batched(all_relations, 1000):
                con.executemany(db.scripts.insert_task_relations, relations)
            con.commit()

            tasks_inserted, = con.execute(db.scripts.count_tasks).fetchone()
            self.log.info(f"wrote {tasks_inserted} tasks")

    @abc.abstractmethod
    def run(self) -> Project:
        self.process_trace()
        self.write_tasks_to_db()
        # self.resolve_return_addresses()
        # if self.debug:
        #     utils.assert_vertex_event_list(self.graph)
            # self.log.info(f"dumping chunks, tasks and graphs to log files")
            # utils.dump_to_log_file(self.chunks, self.graphs, self.task_registry, where=self.abspath(self.debug_dir))
            # graph_log = self.abspath(os.path.join(self.debug_dir, "graph.log"))
            # utils.dump_graph_to_file(self.graph, filename=graph_log, no_flatten=[Task,])
        # self.unpack_vertex_event_attributes()
        # self.cleanup_temporary_attributes()
        # del self.graph.vs['event_list']
        return self


class SimpleProject(Project):
    """Transforms an OTF2 trace into a basic HTML report"""

    def run(self) -> SimpleProject:
        super().run()
        return self
