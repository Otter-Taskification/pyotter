import warnings
from otf2.definitions import Attribute as OTF2Attribute
from otf2 import LocationType as OTF2Location
from collections import Counter
from typing import Iterable, Dict
import otter
from otter.core.event_model import get_event_model
from otter.core.events import Event, Location


def main() -> None:

    args = otter.utils.get_args()
    otter.log.initialise(args)
    log = otter.log.get_logger("main")

    for warning in args.warnings:
        warnings.simplefilter('always', warning)

    log.info(f"reading OTF2 anchorfile: {args.anchorfile}")
    with otter.reader.get_otf2_reader(args.anchorfile) as reader:
        task_registry = otter.core.tasks.TaskRegistry()
        event_model_name = reader.get_event_model_name()
        event_model = get_event_model(event_model_name, task_registry)
        log.info(f"Found event model name: {str(event_model_name)}")
        log.info(f"Using event model: {event_model}")
        log.info(f"generating chunks")
        attributes: Dict[str: OTF2Attribute] = {attr.name: attr for attr in reader.definitions.attributes}
        locations: Dict[OTF2Location: Location] = {location: Location(location) for location in reader.definitions.locations}
        event_iter: Iterable[Event] = (Event(event, locations[location], attributes) for location, event in reader.events)
        chunks = list(event_model.yield_chunks(event_iter))
        # TODO: temporary check, factor out once new event models are passing
        event_model.warn_for_incomplete_chunks(chunks)
        graphs = list(event_model.chunk_to_graph(chunk) for chunk in chunks)

    # Dump chunks and graphs to log file
    if args.loglevel == "DEBUG":
        log.info(f"dumping chunks, tasks and graphs to log files")
        otter.utils.dump_to_log_file(chunks, graphs, task_registry)

    g = event_model.combine_graphs(graphs)

    # Unpack vertex event attributes
    for vertex in g.vs:
        event = vertex['event']
        log.debug(f"unpacking vertex {event=}")
        attributes = otter.core.events.unpack(event)
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
        log.info(f"writing graph to graph.log")
        with open("graph.log", "w") as f:
            f.write("### VERTEX ATTRIBUTES:\n")
            for name in g.vs.attribute_names():
                levels = set(otter.utils.flatten(g.vs[name]))
                n_levels = len(levels)
                if n_levels <= 6:
                    f.write(f"  {name:>35} {n_levels:>6} levels {list(levels)}\n")
                else:
                    f.write(f"  {name:>35} {n_levels:>6} levels (...)\n")

            region_type_count = Counter(g.vs['region_type'])
            region_types = "\n".join([f"{region_type_count[k]:>6} {k}" for k in region_type_count]) + f"\nTotal count: {sum(region_type_count.values())}"

            f.write("\nCount of vertex['region_type'] values:\n")
            f.write(region_types)
            f.write("\n\n")

            f.write("### EDGE ATTRIBUTES:\n")
            for name in g.es.attribute_names():
                levels = set(otter.utils.flatten(g.es[name]))
                n_levels = len(levels)
                if n_levels <= 6:
                    f.write(f"  {name:>35} {n_levels:>6} levels ({list(levels)})\n")
                else:
                    f.write(f"  {name:>35} {n_levels:>6} levels (...)\n")

            f.write("\n")

            f.write("### VERTICES:\n")
            for v in g.vs:
                f.write(f"{v}\n")

            f.write("\n")
            f.write("### EDGES:\n")
            for e in g.es:
                f.write(f"{e.tuple}\n")

    # Clean up temporary vertex attributes
    for name in g.vs.attribute_names():
        if name.startswith("_"):
            del g.vs[name]

    del g.vs['event']

    if args.report:
        otter.styling.style_graph(g)
        otter.styling.style_tasks(task_registry.task_tree())
        otter.reporting.write_report(args, g, task_registry)

    if args.interact:
        otter.utils.interact(locals(), g)

    log.info("Done!")
