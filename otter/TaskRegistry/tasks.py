from collections import deque
from .. import log
from ..log.levels import DEBUG, INFO, WARN, ERROR
from ..EventFactory import events
from ..definitions import Attr
from loggingdecorators import on_init

get_module_logger = log.logger_getter("tasks")


class Task:
    """Represents an instance of a task"""

    @on_init(logger=log.logger_getter("init_logger"))
    def __init__(self, e: events._Event):
        self.logger = get_module_logger()
        data = e.get_task_data()
        self.id = data[Attr.unique_id]
        self.parent_id = data[Attr.parent_task_id]
        if self.parent_id == 18446744073709551615:
            self.parent_id = None
        self.task_type = data[Attr.task_type]
        self.crt_ts = data[Attr.time]
        self._children = deque()

    def __repr__(self):
        return "{}(id={}, type={}, crt_ts={}, parent={}, children=({}))".format(
            self.__class__,
            self.id,
            self.task_type,
            self.crt_ts,
            self.parent_id,
            ", ".join([str(c) for c in self.children])
        )

    def append_child(self, child: int):
        self._children.append(child)

    @property
    def num_children(self):
        return len(self._children)

    @property
    def children(self):
        return (child for child in self._children)

    def keys(self):
        exclude = ["logger"]
        return (key for key in vars(self) if not key in exclude and not key.startswith("_"))

    def as_dict(self):
        return {key: getattr(self, key) for key in self.keys()}
