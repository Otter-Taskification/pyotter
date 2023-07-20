from functools import lru_cache
from itertools import chain
from collections import deque
from typing import Iterable, Deque, List
from dataclasses import dataclass, field, fields, asdict
import igraph as ig
from loggingdecorators import on_init
from .. import log
from ..definitions import NullTaskID, TaskType, SourceLocation

get_module_logger = log.logger_getter("tasks")
VoidLocation = SourceLocation()


class TaskSynchronisationContext:
    """
    Represents a context, such as a taskgroup or taskwait barrier within which
    one or more tasks are synchronised.

    Stores references to tasks which are synchronised at the same point, as well
    as references to iterables containing tasks to synchronise. Iterables are
    evaluated lazily when the context is iterated over.

    A context only includes those tasks explicitly passed to it. It can record
    whether the descendants of those tasks should also be synchronised, although
    it does not record any information about any descendant tasks.
    """

    @on_init(logger=log.logger_getter("init_logger"))
    def __init__(self, tasks=None, descendants: bool = False):
        self.logger = get_module_logger()
        self._tasks = list()
        self._descendants = descendants
        self._iterables = list()
        if tasks is not None:
            self.synchronise_from(tasks)

    def synchronise(self, task):
        """Add a task to the context"""
        assert isinstance(task, Task)
        self.logger.debug(f" -- synchronised task {task.id}")
        self._tasks.append(task)

    def synchronise_lazy(self, iterable):
        """Lazily add an iterable of tasks to the context"""
        self._iterables.append(iterable)

    def synchronise_from(self, iterable):
        """Add tasks from an iterable to the context"""
        for task in iterable:
            self.synchronise(task)

    @property
    def synchronise_descendants(self) -> bool:
        return self._descendants

    def __iter__(self):
        """Yield tasks from the context, including from any iterables added
        lazily"""
        items = chain(self._tasks, *self._iterables)
        for task in items:
            assert isinstance(task, Task)
            yield task

    def __repr__(self):
        return f"{self.__class__.__name__}(descendants={self._descendants})"


@dataclass
class Task:
    id: int
    parent_id: int
    task_flavour: int
    task_label: str
    crt_ts: int
    task_type: TaskType = field(init=False, default=TaskType.explicit)
    init_location: SourceLocation
    start_location: SourceLocation = field(init=False, default=VoidLocation)
    end_location: SourceLocation = field(init=False, default=VoidLocation)
    start_ts: int = field(init=False, default=None)
    end_ts: int = field(init=False, default=None)
    inclusive_duration: int = field(init=False, default=None)
    exclusive_duration: int = field(init=False, default=None)
    naive_duration: int = field(init=False, default=None)

    @on_init(logger=log.logger_getter("init_logger"))
    def __post_init__(self):
        self.logger = get_module_logger()
        self._children: Deque[int] = deque()

        self._num_descendants: int = None

        # Stack of TaskSynchronisationContext to manage nested contexts
        self._task_sync_group_stack: Deque[TaskSynchronisationContext] = deque()

        # Stores child tasks created when parsing a chunk into its graph representation
        self._task_barrier_cache: List[Task] = list()

        # Stores iterables of child tasks created when parsing a chunk into its graph representation
        self._task_barrier_iterables_cache = list()

    def set_start_location(self, location: SourceLocation) -> None:
        if self.start_location is not VoidLocation:
            raise RuntimeError(f"start location was already set: {self}")
        self.logger.debug(f"set task location: start={location}, {self=}")
        self.start_location = location

    def set_end_location(self, location: SourceLocation) -> None:
        if self.end_location is not VoidLocation:
            raise RuntimeError(f"end location was already set: {self}")
        self.logger.debug(f"set task location: end={location}, {self=}")
        self.end_location = location

    def append_child(self, child: int):
        self._children.append(child)

    @property
    def num_children(self):
        return len(self._children)

    @property
    def num_descendants(self):
        return self._num_descendants

    @num_descendants.setter
    def num_descendants(self, n):
        if self._num_descendants is not None:
            raise RuntimeError(f"task {self} already notified of num. descendants")
        self._num_descendants = n

    def iter_children(self) -> Iterable[int]:
        return iter(self._children)

    def set_end_ts(self, time):
        if self.end_ts is not None:
            raise RuntimeError(f"task {self} already notified of end time")
        self.logger.debug(f"{self} end_ts={time}")
        self.end_ts = time
        self.naive_duration = self.end_ts - self.start_ts

    def set_start_ts(self, time):
        if self.start_ts is not None:
            raise RuntimeError(f"task start time already set: {self}")
        self.start_ts = time

    def calculate_naive_duration(self) -> None:
        self.naive_duration = self.end_ts - self.start_ts

    def as_dict(self):
        return asdict(self)

    def is_implicit(self) -> bool:
        return self.task_type == TaskType.implicit

    def is_explicit(self) -> bool:
        return self.task_type == TaskType.explicit

    def append_to_barrier_cache(self, task):
        # TODO: consider having TaskRegistry manage barrier caches - why should a Task know about them?
        """Add a task to the barrier cache, ready to be passed to a synchronisation
        context upon encountering a task synchronisation barrier
        """
        assert isinstance(task, Task)
        self.logger.debug(
            f"add task to barrier cache: task {self.id} added task {task.id}"
        )
        self._task_barrier_cache.append(task)

    def append_to_barrier_iterables_cache(self, iterable):
        # TODO: consider having TaskRegistry manage barrier caches - why should a Task know about them?
        """Add an iterable to the barrier iterables cache, ready to be passed to a synchronisation
        context upon encountering a task synchronisation barrier
        """
        self.logger.debug(
            f"add iterable to iterables cache: task {self.id} added iterable"
        )
        self._task_barrier_iterables_cache.append(iterable)

    def synchronise_tasks_at_barrier(
        self, tasks=None, from_cache=False, descendants=False
    ):
        # TODO: consider having TaskRegistry manage barrier caches - why should a Task know about them?
        """At a task synchronisation barrier, add a set of tasks to a synchronisation
        context. The tasks may be from the internal cache, or supplied externally.
        """
        tasks = self._task_barrier_cache if from_cache else tasks
        assert tasks is not None
        self.logger.debug(f"task {self.id} registering tasks synchronised at barrier")
        barrier_context = TaskSynchronisationContext(tasks, descendants=descendants)
        if from_cache:
            self.clear_task_barrier_cache()
        return barrier_context

    # TODO: consider having TaskRegistry or some special manager class handle this
    @property
    def task_barrier_cache(self):
        return self._task_barrier_cache

    # TODO: consider having TaskRegistry or some special manager class handle this
    def clear_task_barrier_cache(self):
        self._task_barrier_cache.clear()

    # TODO: consider having TaskRegistry or some special manager class handle this
    @property
    def task_barrier_iterables_cache(self):
        return self._task_barrier_iterables_cache

    # TODO: consider having TaskRegistry or some special manager class handle this
    def clear_task_barrier_iterables_cache(self):
        self._task_barrier_iterables_cache.clear()

    # TODO: consider having TaskRegistry or some special manager class handle this
    @property
    def has_active_task_group(self):
        return self.num_enclosing_task_sync_groups > 0

    # TODO: consider having TaskRegistry or some special manager class handle this
    @property
    def num_enclosing_task_sync_groups(self):
        return len(self._task_sync_group_stack)

    # TODO: consider having TaskRegistry or some special manager class handle this
    def synchronise_task_in_current_group(self, task):
        if self.has_active_task_group:
            # get enclosing group context
            self.logger.debug(
                f"task {self.id} registering task {task.id} in current group"
            )
            group_context = self.get_current_task_sync_group()
            group_context.synchronise(task)

    # TODO: consider having TaskRegistry or some special manager class handle this
    def enter_task_sync_group(self, descendants=True):
        self.logger.debug(
            f"task {self.id} entering task sync group (levels={self.num_enclosing_task_sync_groups})"
        )
        group_context = TaskSynchronisationContext(tasks=None, descendants=descendants)
        self._task_sync_group_stack.append(group_context)

    # TODO: consider having TaskRegistry or some special manager class handle this
    def leave_task_sync_group(self):
        assert self.has_active_task_group
        group_context = self._task_sync_group_stack.pop()
        self.logger.debug(
            f"task {self.id} left task sync group (levels={self.num_enclosing_task_sync_groups})"
        )
        return group_context

    # TODO: consider having TaskRegistry or some special manager class handle this
    def get_current_task_sync_group(self):
        assert self.has_active_task_group
        self.logger.debug(
            f"task {self.id} entering task sync group (levels={self.num_enclosing_task_sync_groups})"
        )
        group_context = self._task_sync_group_stack[-1]
        return group_context


_task_field_names: List[str] = [f.name for f in fields(Task)]


class _NullTask(Task):
    _instance: Task = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self, *args, **kwargs):
        # Deliberately don't init as this represents the absence of a task e.g. parent of the root task
        pass


NullTask = _NullTask()


class TaskRegistry(Iterable[Task]):
    """
    Maintains references to all tasks encountered in a trace
    Maps task ID to task instance, raising KeyError if an unregistered task is requested
    """

    @on_init(logger=log.logger_getter("init_logger"))
    def __init__(self):
        self.log = get_module_logger()
        self._dict = dict()

    def __getitem__(self, uid: int) -> Task:
        assert isinstance(uid, int)
        if uid not in self._dict:
            if uid == NullTaskID:
                # raise NullTaskError()
                return NullTask
            else:
                raise KeyError(f"task {uid} was not found in {self}")
        return self._dict[uid]

    def __iter__(self) -> Iterable[Task]:
        for task in self._dict.values():
            yield task

    def __len__(self):
        return len(self._dict)

    def __repr__(self):
        return f"{self.__class__.__name__}({len(self._dict.keys())} tasks: {list(self._dict.keys())})"

    def register_task(self, task: Task) -> Task:
        self.log.debug(
            f"registering task {task.id} (parent={task.parent_id if task.id>0 else None})"
        )
        if task.id in self._dict:
            raise ValueError(f"task {task.id} was already registered in {self}")
        self._dict[task.id] = task
        # if the task's parent was recorded as NULL at runtime, t.parent_id is None
        if task.id > 0 and task.parent_id is not None:
            parent_task = self[task.parent_id]
            if parent_task is not NullTask:
                parent_task.append_child(task.id)
        return task

    def update_task(self, event) -> Task:
        """Update the task data"""
        raise NotImplementedError()

    @lru_cache(maxsize=None)
    def descendants_while(self, task_id, cond):
        return list(self._generate_descendants_while(task_id, cond))

    def _generate_descendants_while(self, task_id, cond):
        for child_id in self[task_id].iter_children():
            if cond is not None and cond(self[child_id]):
                yield child_id
                yield from self.descendants_while(child_id, cond)

    @lru_cache(maxsize=None)
    def task_tree(self):
        task_tree = ig.Graph(n=len(self), directed=True)
        task_tree.vs["name"] = list(task.id for task in self)
        task_tree.vs["task"] = list(self[id] for id in task_tree.vs["name"])
        for task in self:
            for child in task.iter_children():
                task_tree.add_edge(task.id, child)
        return task_tree

    @property
    def attributes(self):
        return _task_field_names

    @property
    def data(self):
        return (task.as_dict() for task in self)

    def calculate_all_inclusive_duration(self):
        for task in self:
            if task.inclusive_duration is None:
                task.inclusive_duration = self.calculate_inclusive_duration(task)

    def calculate_inclusive_duration(self, task):
        if task.inclusive_duration is not None:
            return task.inclusive_duration
        inclusive_duration = task.exclusive_duration
        for child in (self[taskid] for taskid in task.iter_children()):
            if child.inclusive_duration is None:
                child.inclusive_duration = self.calculate_inclusive_duration(child)
            inclusive_duration += child.inclusive_duration
        return inclusive_duration

    def calculate_all_num_descendants(self):
        for task in self:
            if task.num_descendants is None:
                task.num_descendants = self.calculate_num_descendants(task)

    def calculate_num_descendants(self, task):
        descendants = task.num_children
        for child in (self[child_id] for child_id in task.iter_children()):
            if child.num_descendants is None:
                child.num_descendants = self.calculate_num_descendants(child)
            descendants += child.num_descendants
        return descendants

    def notify_task_start(
        self, task_id: int, time: int, location: SourceLocation
    ) -> None:
        task = self[task_id]
        if task.start_ts is None:
            task.start_ts = time
        if location is not None:
            task.set_start_location(location)

    def update_task_duration(
        self, prior_task_id: int, next_task_id: int, time: int
    ) -> None:
        prior_task = self[prior_task_id]
        if prior_task is not NullTask:
            # self.log.debug(f"got prior task: {prior_task}")
            prior_task.update_exclusive_duration(time)
        next_task = self[next_task_id]
        if next_task is not NullTask:
            # self.log.debug(f"got next task: {next_task}")
            next_task.resumed_at(time)

    def notify_task_end(
        self, completed_task_id: int, time: int, location: SourceLocation
    ) -> None:
        completed_task = self[completed_task_id]
        if completed_task is not NullTask:
            completed_task.end_ts = time
            completed_task.calculate_naive_duration()
        if location is not None:
            completed_task.set_end_location(location)

    def log_all_task_ts(self):
        self.log.debug("BEGIN LOGGING TASK TIMESTAMPS")
        for task in self:
            self.log.debug(f"{str(task.id):>6}:{task.start_ts}:{task.end_ts}")
        self.log.debug("END LOGGING TASK TIMESTAMPS")
