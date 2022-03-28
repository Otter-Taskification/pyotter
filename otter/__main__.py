import otf2
import otter

args = otter.get_args()
otter.Logging.initialise(args)
log = otter.get_logger('MAIN')

log.info(f"{args.anchorfile=}")
with otf2.reader.open(args.anchorfile) as r:
    events = otter.EventFactory(r)
    tasks = otter.TaskRegistry()
    chunks = otter.ChunkFactory(events, tasks)
    log.debug(f"iterating over chunks in {chunks}")
    for chunk in chunks:
        pass

log.info("Done!")
