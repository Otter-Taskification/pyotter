from collections import defaultdict, deque
from functools import cached_property
import loggingdecorators as logdec
from ... import log
from . import chunks
from .. import tasks

get_module_logger = log.logger_getter("chunks")

class ChunkFactory:
    """Aggregates a sequence of events into a sequence of Chunks."""

    @logdec.on_init(logger=log.logger_getter("init_logger"))
    def __init__(self, events, tasks):
        self.log = get_module_logger()
        self.events = events
        self.tasks = tasks
        # Track all chunks currently under construction according to key
        self.chunk_dict = defaultdict(lambda : chunks.Chunk())
        # Record the enclosing chunk when an event indicates a nested chunk
        self.chunk_stack = defaultdict(deque)

    def __repr__(self):
        return f"{self.__class__.__name__}({len(self.chunk_dict)} chunks)"

    def __iter__(self) -> chunks.Chunk:
        self.log.debug(f"{self.__class__.__name__}.__iter__ receiving events from {self.events}")
        for k, event in enumerate(self.events):
            self.log.debug(f"got event {k}: {event}")

            if event.is_chunk_switch_event:
                self.log.debug(f"updating chunks")
                yield from event.update_chunks(self.chunk_dict, self.chunk_stack)
            else:
                self.chunk_dict[event.encountering_task_id].append_event(event)

            if event.is_task_register_event:
                self.tasks.register_task(event)

            if event.is_task_complete_event:
                completed_task_id = event.get_task_completed()
                self.log.debug(f"event <{event}> notifying task {completed_task_id} of end_ts")
                try:
                    completed_task = self.tasks[completed_task_id]
                except tasks.NullTaskError:
                    pass
                else:
                    completed_task.end_ts = event.time

        self.log.debug(f"exhausted {self.events}")

    def read(self):
        yield from filter(None, self)

    @cached_property
    def chunks(self):
        return list(self.read())
