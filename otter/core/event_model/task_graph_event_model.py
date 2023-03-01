from .event_model import EventModelFactory, BaseEventModel
from typing import Iterable
from otter.definitions import EventModel
from otter.core.chunks import Chunk
from otter.core.events import Event


@EventModelFactory.register(EventModel.TASKGRAPH)
class TaskGraphEventModel(BaseEventModel):

    def yield_chunks(self, events: Iterable[Event], use_core: bool=True) -> Iterable[Chunk]:
        # Will replace otter.chunks.yield_chunks
        raise NotImplementedError()

    def chunk_to_graph(self, chunk):
        raise NotImplementedError()

    def combine_graphs(self, graphs):
        raise NotImplementedError()


def is_chunk_switch_event(event: Event) -> bool:
    return event.is_chunk_switch_event
