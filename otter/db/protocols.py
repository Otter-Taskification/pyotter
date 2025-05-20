from typing import Protocol, Optional, Tuple, Iterable

from otter.otf2_ext.events import EventType

from otter.definitions import TaskAction, TaskID
from otter.core.events import Event

from .types import SourceLocation


class TaskMetaCallback(Protocol):
    """Callback used to dispatch task metadata"""

    def __call__(self, task: TaskID, parent: Optional[TaskID], label: str) -> None: ...


class TaskActionCallback(Protocol):
    """Callback used to dispatch task action data"""

    def __call__(
        self,
        task: TaskID,
        action: TaskAction,
        time: int,
        source_location: SourceLocation,
        /,
        *,
        location_ref: Optional[int] = None,
        location_count: Optional[int] = None,
        cpu: int,
        tid: int,
    ) -> None: ...


class TaskSuspendMetaCallback(Protocol):
    """Callback used to dispatch metadata about task-suspend actions"""

    def __call__(self, task: TaskID, time: int, sync_descendants: bool, sync_mode: int) -> None: ...


class CriticalTaskCallback(Protocol):
    """Callback used to notify that a given child is the critical task during part of a parent's execution"""

    def __call__(self, task: TaskID, sequence: int, critical_child: TaskID, /, *args) -> None: ...


class EventReaderProtocol(Protocol):
    """Responsible for reading or re-constructing the events of a task"""

    def __call__(self, task: TaskID, /) -> list[Event]: ...


class SeekEventsCallback(Protocol):
    """A callback used to seek events from a trace, given tuples of location ref
    & event positions
    """

    def __call__(
        self, positions: Iterable[Tuple[int, int]], batch_size: int = 100
    ) -> Iterable[Tuple[int, Tuple[int, EventType]]]: ...
