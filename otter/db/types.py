from dataclasses import dataclass, InitVar, field, asdict
from typing import NamedTuple, Callable, Optional

from otter.definitions import TaskAction, TaskID, TaskSyncMode


class SourceLocation(NamedTuple):
    file: str = "?"
    func: str = "?"
    line: int = 0

    def __str__(self) -> str:
        return f"{self.file}:{self.line} in {self.func}"


@dataclass(frozen=True)
class Event:
    time: int
    task: TaskID
    action: TaskAction
    location: SourceLocation
    cpu: int
    thread: int
    sync_mode: Optional[TaskSyncMode] # Only populated where action is TaskAction.SUSPEND


@dataclass(frozen=True)
class TaskSchedulingState:
    task: TaskID
    action_start_int: InitVar[int]
    action_end_int: InitVar[int]
    file_name_start: InitVar[str]
    func_name_start: InitVar[str]
    line_start: InitVar[int]
    file_name_end: InitVar[str]
    func_name_end: InitVar[str]
    line_end: InitVar[int]
    start_ts: int
    end_ts: int
    cpu_start: int
    cpu_end: int
    tid_start: int
    tid_end: int
    duration: int
    start_location: SourceLocation = field(init=False)
    end_location: SourceLocation = field(init=False)
    action_start: TaskAction = field(init=False)
    action_end: TaskAction = field(init=False)

    @property
    def is_active(self):
        return self.action_start in (TaskAction.START, TaskAction.RESUME)

    def __post_init__(
        self,
        action_start_int: int,
        action_end_int: int,
        file_name_start: str,
        func_name_start: str,
        line_start: int,
        file_name_end: str,
        func_name_end: str,
        line_end: int,
    ) -> None:
        super().__setattr__(
            "start_location",
            SourceLocation(file_name_start, func_name_start, line_start),
        )
        super().__setattr__(
            "end_location",
            SourceLocation(file_name_end, func_name_end, line_end),
        )
        super().__setattr__("action_start", TaskAction(action_start_int))
        super().__setattr__("action_end", TaskAction(action_end_int))

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(task={self.task}, {self.action_start}:{self.start_location} at {self.start_ts} -> {self.action_end}:{self.end_location} at {self.end_ts})"

    def asdict(
        self,
        *,
        location: Optional[Callable] = str,
        action: Optional[Callable] = lambda x: TaskAction(x).name,
    ):
        d = asdict(self)
        if location:
            d["start_location"] = location(self.start_location)
            d["end_location"] = location(self.end_location)
        if action:
            d["action_start"] = action(self.action_start)
            d["action_end"] = action(self.action_end)
        return d


@dataclass(frozen=True)
class TaskAttributes:  # For use in Connection.parent_child_attributes
    # Describes the invariant attributes of a task i.e. those compiled into the annotations

    label: str
    create_location: SourceLocation
    start_location: SourceLocation
    end_location: SourceLocation

    def is_null(self) -> bool:
        return self.label is None

    def asdict(self):
        return asdict(self)


@dataclass(frozen=True)
class Task:
    # Represents a row in the task table

    id: TaskID
    parent: TaskID
    children: int
    create_ts: int
    start_ts: int
    end_ts: int
    label: InitVar[str]
    create_location: InitVar[SourceLocation]
    start_location: InitVar[SourceLocation]
    end_location: InitVar[SourceLocation]
    attr: TaskAttributes = field(init=False)

    def __post_init__(
        self,
        label: str,
        create_location: SourceLocation,
        start_location: SourceLocation,
        end_location: SourceLocation,
    ) -> None:
        super().__setattr__(
            "attr",
            TaskAttributes(
                label,
                create_location,
                start_location,
                end_location,
            ),
        )

    def is_null(self) -> bool:
        return self.attr.is_null()

    def asdict(self, *, flatten: bool = False, location: Optional[Callable] = str):
        if not flatten:
            return asdict(self)
        d = asdict(self)
        del d["attr"]
        d["label"] = self.attr.label
        for locname in ["create_location", "start_location", "end_location"]:
            loc = getattr(self.attr, locname)
            d[locname] = location(loc) if location else loc
        return d
