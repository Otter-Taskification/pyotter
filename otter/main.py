# TODO: include new graph styling methods from taskgraph to otter top level package
# TODO: add TEMPORARY vertex["_event.attributes"] as the dict of event attributes

import warnings
from otf2.definitions import Attribute as OTF2Attribute
from otf2 import LocationType as OTF2Location
from typing import Iterable, Dict, Tuple
import otter
from otter.core.event_model import get_event_model
from otter.core.events import Event, Location
from otter.definitions import EventModel
from otter import graph_styling
from igraph import Graph


def main() -> None:

    args = otter.utils.get_args()
    otter.log.initialise(args)
    log = otter.log.get_logger("main")

    for warning in args.warnings:
        warnings.simplefilter('always', warning)

    log.info(f"reading OTF2 anchorfile: {args.anchorfile}")
    with otter.reader.get_otf2_reader(args.anchorfile) as reader:
        task_registry = otter.core.tasks.TaskRegistry()
        event_model_name: EventModel = reader.get_event_model_name()
        event_model = get_event_model(event_model_name, task_registry)
        log.info(f"Found event model name: {str(event_model_name)}")
        log.info(f"Using event model: {event_model}")
        log.info(f"generating chunks")
        attributes: Dict[str: OTF2Attribute] = {attr.name: attr for attr in reader.definitions.attributes}
        locations: Dict[OTF2Location: Location] = {location: Location(location) for location in reader.definitions.locations}
        event_iter: Iterable[Tuple[Location, Event]] = ((locations[location], Event(event, attributes)) for location, event in reader.events)
        chunks = list(event_model.yield_chunks(event_iter))
        # TODO: temporary check, factor out once new event models are passing
        event_model.warn_for_incomplete_chunks(chunks)
        graphs = list(event_model.chunk_to_graph(chunk) for chunk in chunks)

    # Dump chunks and graphs to log file
    if args.loglevel == "DEBUG":
        log.info(f"dumping chunks, tasks and graphs to log files")
        otter.utils.dump_to_log_file(chunks, graphs, task_registry)

    graph = event_model.combine_graphs(graphs)

    # vertex['event_list'] should always be a list of 1 or more events
    if args.loglevel == "DEBUG":
        otter.utils.assert_vertex_event_list(graph)

    # Unpack vertex event attributes
    for vertex in graph.vs:
        event_list = vertex['event_list']
        log.debug(f"unpacking vertex event_list:")
        if args.loglevel == "DEBUG":
            for event in event_list:
                log.debug(f"  {event}")
        attributes = event_model.unpack(event_list)
        for key, value in attributes.items():
            log.debug(f"  got {key}={value}")
            if isinstance(value, list):
                s = set(value)
                if len(s) == 1:
                    value = s.pop()
                else:
                    log.debug(f"  concatenate {len(value)} values")
                    value = ";".join(str(item) for item in value)
            if isinstance(value, int):
                value = str(value)
            elif value == "":
                value = None
            log.debug(f"    unpacked {value=}")
            vertex[key] = value

    # Dump graph details to file
    if args.loglevel == "DEBUG":
        otter.utils.dump_graph_to_file(graph)

    # Clean up temporary vertex attributes
    for name in graph.vs.attribute_names():
        if name.startswith("_"):
            del graph.vs[name]

    del graph.vs['event_list']

    if args.report:
        otter.styling.style_graph(graph)
        otter.styling.style_tasks(task_registry.task_tree())
        otter.reporting.write_report(args, graph, task_registry)
        log.info(f"report written to {args.report}")

    if args.interact:
        otter.utils.interact(locals(), graph)
