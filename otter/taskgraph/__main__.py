import otf2
import otter
from otter import taskgraph as tg
from otter.taskgraph import defn
from otter.reporting import report
import igraph as ig

args = otter.get_args()
otter.log.initialise(args)
log = otter.log.get_logger("main")

"""
NOTE:
    - probably want to adopt the "chunk" model now to handle sequential taskwait barriers
    - at the moment we just naively connect tasks to their parent's vertices
    - synchronised tasks are connected to the taskwait vertex
    - successive taskwait barriers are not connected in sequence but are (incorrectly) connected as siblings i.e. with no order enforced
"""

log.info(f"reading OTF2 anchorfile: {args.anchorfile}")
with otf2.reader.open(args.anchorfile) as otf2_reader:
    event_reader = tg.EventReader(otf2_reader)
    event_graph = tg.EventGraph(style=tg.styling.VertexAsHTMLTableStyle())
    event_graph.from_events(event_reader.events)
    event_graph.finalise_graph()
    event_graph.apply_styling()
    event_graph.save_as_svg("graph.svg")

if args.interact:
    otter.interact(locals(), event_graph.graph)
