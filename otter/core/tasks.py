from functools import lru_cache
from itertools import chain
from collections import deque
from typing import Dict, Tuple, List, Any, AnyStr, Iterable, Set
import igraph as ig
from loggingdecorators import on_init
from .. import log
from ..definitions import Attr, AttrValue, NullTaskID, TaskType, SourceLocation

get_module_logger = log.logger_getter("tasks")


TaskData = Dict[Attr, AttrValue]


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
    def __init__(self, tasks=None, descendants=False):
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
    def synchronise_descendants(self):
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


class Task:
    """Represents an instance of a task"""

    # TODO: clean up this API, it's too confusing which attributes are public and which are not. What attributes are available through a Task?

    """A list of symbols exported by a Task through its .keys() method. These must return something when requested via getattr"""
    _properties_ = ["id",
        "start_ts",
        "end_ts",
        "exclusive_duration",
        "inclusive_duration",
        "num_children",
        "num_descendants",

        # "source_file_name",
        # "source_func_name",
        # "source_line_number,"

        "initialised_at",
        "started_at",
        "ended_at"
    ]

    @on_init(logger=log.logger_getter("init_logger"))
    def __init__(self, task_data: TaskData):
        self.logger = get_module_logger()
        self.id = task_data[Attr.unique_id]
        self.parent_id = task_data[Attr.parent_task_id]
        if self.parent_id == NullTaskID:
            self.parent_id = None
        self.task_type = task_data[Attr.task_type]
        self.flavour = task_data[Attr.task_flavour]
        self.user_label = task_data[Attr.task_label]
        self.crt_ts = task_data[Attr.time]

        # TODO: want to remove these attributes in favour of source_* and task_init*
        # Source location attributes not defined in OMP traces
        # self.source_file_name = task_data.get(Attr.source_file_name)
        # self.source_func_name = task_data.get(Attr.source_func_name)
        # self.source_line_number = task_data.get(Attr.source_line_number)

        # The location where the task was initialised
        self._init_location: SourceLocation = SourceLocation(
            task_data[Attr.task_init_file],
            task_data[Attr.task_init_func],
            task_data[Attr.task_init_line]
        )

        # The location where the task started
        self._start_location: SourceLocation = None

        # The location where the task ended
        self._end_location: SourceLocation = None

        self._children = deque()
        self._end_ts = None
        self._last_resumed_ts = None
        self._excl_dur = 0
        self._incl_dur = None
        self._naive_duration: int = None
        self._num_descendants = None
        self._start_ts = None

        # Stack of TaskSynchronisationContext to manage nested contexts
        self._task_sync_group_stack = deque()

        # Stores child tasks created when parsing a chunk into its graph representation
        self._task_barrier_cache = list()

        # Stores iterables of child tasks created when parsing a chunk into its graph representation
        self._task_barrier_iterables_cache = list()

    def __repr__(self):
        return "{}(id={}, type={}, crt_ts={}, end_ts={}, parent={}, children=({}))".format(
            self.__class__,
            self.id,
            self.task_type,
            self.crt_ts,
            self.end_ts,
            self.parent_id,
            ", ".join([str(c) for c in self.children])
        )

    def __iter__(self) -> Tuple[Any]:
        return tuple(getattr(self, name) for name in self._properties_)

    @classmethod
    def properties(cls) -> List[AnyStr]:
        return cls._properties_

    def append_to_barrier_cache(self, task):
        """Add a task to the barrier cache, ready to be passed to a synchronisation
        context upon encountering a task synchronisation barrier
        """
        assert isinstance(task, Task)
        self.logger.debug(f"add task to barrier cache: task {self.id} added task {task.id}")
        self._task_barrier_cache.append(task)

    def append_to_barrier_iterables_cache(self, iterable):
        """Add an iterable to the barrier iterables cache, ready to be passed to a synchronisation
        context upon encountering a task synchronisation barrier
        """
        self.logger.debug(f"add iterable to iterables cache: task {self.id} added iterable")
        self._task_barrier_iterables_cache.append(iterable)

    def synchronise_tasks_at_barrier(self, tasks=None, from_cache=False, descendants=False):
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

    @property
    def start_location(self) -> SourceLocation:
        if self._start_location is None:
            raise ValueError(f"start location was never set: {self}")
        return self._start_location

    @start_location.setter
    def start_location(self, location: SourceLocation) -> None:
        if self._start_location is not None:
            raise RuntimeError(f"start location was already set: {self}")
        self.logger.debug(f"set task location: start={location}, {self=}")
        self._start_location = location

    @property
    def end_location(self) -> SourceLocation:
        if self._end_location is None:
            raise ValueError(f"end location was never set: {self}")
        return self._end_location

    @end_location.setter
    def end_location(self, location: SourceLocation) -> None:
        if self._end_location is not None:
            raise RuntimeError(f"end location was already set: {self}")
        self.logger.debug(f"set task location: end={location}, {self=}")
        self._end_location = location

    @property
    def started_at(self) -> SourceLocation:
        return self.start_location

    @property
    def ended_at(self) -> SourceLocation:
        return self.end_location

    @property
    def initialised_at(self) -> SourceLocation:
        return self._init_location

    @property
    def task_barrier_cache(self):
        return self._task_barrier_cache

    def clear_task_barrier_cache(self):
        self._task_barrier_cache.clear()

    @property
    def task_barrier_iterables_cache(self):
        return self._task_barrier_iterables_cache

    def clear_task_barrier_iterables_cache(self):
        self._task_barrier_iterables_cache.clear()

    @property
    def has_active_task_group(self):
        return self.num_enclosing_task_sync_groups > 0

    @property
    def num_enclosing_task_sync_groups(self):
        return len(self._task_sync_group_stack)

    def synchronise_task_in_current_group(self, task):
        if self.has_active_task_group:
            # get enclosing group context
            self.logger.debug(f"task {self.id} registering task {task.id} in current group")
            group_context = self.get_current_task_sync_group()
            group_context.synchronise(task)

    def enter_task_sync_group(self, descendants=True):
        self.logger.debug(f"task {self.id} entering task sync group (levels={self.num_enclosing_task_sync_groups})")
        group_context = TaskSynchronisationContext(tasks=None, descendants=descendants)
        self._task_sync_group_stack.append(group_context)

    def leave_task_sync_group(self):
        assert self.has_active_task_group
        group_context = self._task_sync_group_stack.pop()
        self.logger.debug(f"task {self.id} left task sync group (levels={self.num_enclosing_task_sync_groups})")
        return group_context

    def get_current_task_sync_group(self):
        assert self.has_active_task_group
        self.logger.debug(f"task {self.id} entering task sync group (levels={self.num_enclosing_task_sync_groups})")
        group_context = self._task_sync_group_stack[-1]
        return group_context

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

    @property
    def children(self):
        return (child for child in self._children)

    @property
    def end_ts(self):
        return self._end_ts

    @end_ts.setter
    def end_ts(self, time):
        if self._end_ts is not None:
            raise RuntimeError(f"task {self} already notified of end time")
        self.logger.debug(f"{self} end_ts={time}")
        self._end_ts = time
        self._naive_duration = self.end_ts - self.start_ts

    @property
    def start_ts(self):
        return self._start_ts

    @start_ts.setter
    def start_ts(self, time):
        if self.start_ts is not None:
            raise RuntimeError(f"task start time already set: {self}")
        self._start_ts = time

    @property
    def last_resumed_ts(self):
        return self._last_resumed_ts

    def resumed_at(self, time):
        self._last_resumed_ts = time
        self.logger.debug(f"resumed task {self.id}, duration={self.exclusive_duration}")

    @property
    def exclusive_duration(self):
        return self._excl_dur

    def update_exclusive_duration(self, time):
        if self.last_resumed_ts is None:
            raise RuntimeError("last resumed time was None")
        dt = time - self.last_resumed_ts
        self.logger.debug(f"updated exclusive duration for task {self.id}: {self._excl_dur} -> {self._excl_dur + dt}")
        self._excl_dur = self._excl_dur + dt

    @property
    def inclusive_duration(self):
        return self._incl_dur

    @inclusive_duration.setter
    def inclusive_duration(self, dt):
        self._incl_dur = dt

    @property
    def naive_duration(self) -> int:
        return self._naive_duration

    def keys(self):
        exclude = ["logger"]
        names = list(vars(self).keys()) + self._properties_
        return (name for name in names if not name in exclude and not name.startswith("_"))

    def as_dict(self):
        return {key: getattr(self, key) for key in self.keys()}

    def is_implicit(self) -> bool:
        return self.task_type == TaskType.implicit

    def is_explicit(self) -> bool:
        return self.task_type == TaskType.explicit


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
        self._task_attributes = list()
        self._task_attribute_set = set()

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

    def register_task(self, task_data: TaskData) -> Task:
        t = Task(task_data)
        self.log.debug(f"registering task {t.id} (parent={t.parent_id if t.id>0 else None})")
        if t.id in self._dict:
            raise ValueError(f"task {t.id} was already registered in {self}")
        self._dict[t.id] = t
        # if the task's parent was recorded as NULL at runtime, t.parent_id is None
        if t.id > 0 and t.parent_id is not None:
            self[t.parent_id].append_child(t.id)
        for name in t.keys():
            if name not in self._task_attribute_set:
                self._task_attribute_set.add(name)
                self._task_attributes.append(name)
        return t

    def update_task(self, event) -> Task:
        """Update the task data"""
        raise NotImplementedError()

    @lru_cache(maxsize=None)
    def descendants_while(self, task_id, cond):
        return list(self._generate_descendants_while(task_id, cond))

    def _generate_descendants_while(self, task_id, cond):
        for child_id in self[task_id].children:
            if cond is not None and cond(self[child_id]):
                yield child_id
                yield from self.descendants_while(child_id, cond)

    @lru_cache(maxsize=None)
    def task_tree(self):
        task_tree = ig.Graph(n=len(self), directed=True)
        task_tree.vs['name'] = list(task.id for task in self)
        task_tree.vs['task'] = list(self[id] for id in task_tree.vs['name'])
        for task in self:
            for child in task.children:
                task_tree.add_edge(task.id, child)
        return task_tree

    @property
    def attributes(self):
        return self._task_attributes

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
        for child in (self[id] for id in task.children):
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
        for child in (self[id] for id in task.children):
            if child.num_descendants is None:
                child.num_descendants = self.calculate_num_descendants(child)
            descendants += child.num_descendants
        return descendants

    def notify_task_start(self, task_id: int, time: int, location: SourceLocation) -> None:
        task = self[task_id]
        if task.start_ts is None:
            task.start_ts = time
        if location is not None:
            task.start_location = location

    def update_task_duration(self, prior_task_id: int, next_task_id: int, time: int) -> None:
        prior_task = self[prior_task_id]
        if prior_task is not NullTask:
            # self.log.debug(f"got prior task: {prior_task}")
            prior_task.update_exclusive_duration(time)
        next_task = self[next_task_id]
        if next_task is not NullTask:
            # self.log.debug(f"got next task: {next_task}")
            next_task.resumed_at(time)

    def notify_task_end(self, completed_task_id: int, time: int, location: SourceLocation) -> None:
        completed_task = self[completed_task_id]
        if completed_task is not NullTask:
            completed_task.end_ts = time
        if location is not None:
            completed_task.end_location = location

    def log_all_task_ts(self):
        self.log.debug("BEGIN LOGGING TASK TIMESTAMPS")
        for task in self:
            self.log.debug(f"{str(task.id):>6}:{task.start_ts}:{task.end_ts}")
        self.log.debug("END LOGGING TASK TIMESTAMPS")
