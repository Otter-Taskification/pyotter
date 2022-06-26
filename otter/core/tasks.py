from functools import lru_cache
from collections import deque
import igraph as ig
import loggingdecorators as logdec
from .. import log
from .. import definitions as defn

get_module_logger = log.logger_getter("tasks")


class NullTaskError(Exception):
    pass


class Task:
    """Represents an instance of a task"""

    @logdec.on_init(logger=log.logger_getter("init_logger"))
    def __init__(self, event):
        self.logger = get_module_logger()
        data = event.get_task_data()
        self.id = data[defn.Attr.unique_id]
        self.parent_id = data[defn.Attr.parent_task_id]
        if self.parent_id == defn.NullTask:
            self.parent_id = None
        self.task_type = data[defn.Attr.task_type]
        self.crt_ts = data[defn.Attr.time]

        # Source location attributes not defined in OMP traces
        self.source_file_name = data.get(defn.Attr.source_file_name)
        self.source_func_name = data.get(defn.Attr.source_func_name)
        self.source_line_number = data.get(defn.Attr.source_line_number)
        
        self._children = deque()
        self._end_ts = None
        self._last_resumed_ts = None
        self._excl_dur = 0
        self._incl_dur = None
        self._num_descendants = None
        self._start_ts = None

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

    def keys(self):
        exclude = ["logger"]
        properties = ["start_ts",
            "end_ts",
            "exclusive_duration",
            "inclusive_duration",
            "num_children",
            "num_descendants",
            "source_file_name",
            "source_func_name",
            "source_line_number"
        ]
        names = list(vars(self).keys()) + properties
        return (name for name in names if not name in exclude and not name.startswith("_"))

    def as_dict(self):
        return {key: getattr(self, key) for key in self.keys()}


class TaskRegistry:
    """
    Maintains references to all tasks encountered in a trace
    Maps task ID to task instance, raising KeyError if an unregistered task is requested
    """

    @logdec.on_init(logger=log.logger_getter("init_logger"))
    def __init__(self):
        self.log = get_module_logger()
        self._dict = dict()
        self._task_attributes = list()
        self._task_attribute_set = set()

    def __getitem__(self, uid: int) -> Task:
        assert isinstance(uid, int)
        if uid not in self._dict:
            if uid == defn.NullTask:
                raise NullTaskError()
            else:
                raise KeyError(f"task {uid} was not found in {self}")
        return self._dict[uid]

    def __iter__(self):
        for task in self._dict.values():
            yield task

    def __len__(self):
        return len(self._dict)

    def __repr__(self):
        return f"{self.__class__.__name__}({len(self._dict.keys())} tasks: {list(self._dict.keys())})"

    def register_task(self, event) -> Task:
        t = Task(event)
        self.log.debug(f"registering task {t.id} (parent={t.parent_id if t.id>0 else None})")
        if t.id in self._dict:
            raise ValueError(f"task {t.id} was already registered in {self}")
        self._dict[t.id] = t
        if t.id > 0:
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
