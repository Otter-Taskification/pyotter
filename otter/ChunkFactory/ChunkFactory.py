from .. import EventFactory, TaskRegistry
from . import chunks
from collections import defaultdict, deque
from ..logging import get_logger
from .. import utils

class ChunkFactory:
    """Aggregates a sequence of events into a sequence of Chunks."""

    @utils.decorate.log_init
    def __init__(self, events: EventFactory, tasks: TaskRegistry):
        self.log = get_logger(f"{self.__class__.__name__}")
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
            self.log.debug(f"{self.__class__.__name__}.__iter__ got event {k}: {event}")
            if event.is_task_register_event:
                self.tasks.register_task(event)
            if event.is_chunk_switch_event:
                yield from event.update_chunks(self.chunk_dict, self.chunk_stack)
            else:
                self.chunk_dict[event.encountering_task_id].append_event(event)
            self.log.debug(f"{self.__class__.__name__}.__iter__ requesting next event")
        self.log.debug(f"{self.__class__.__name__}.__iter__ exhausted {self.events}")

    def read(self):
        yield from filter(None, self)
