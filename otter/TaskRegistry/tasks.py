from collections import deque
from .. import log
from ..log import get_logger
from ..log.levels import DEBUG, INFO, WARN, ERROR
from ..EventFactory import events
from ..definitions import Attr
from loggingdecorators import on_init

module_logger = get_logger("tasks")


class Task:
    """Represents an instance of a task"""

    @on_init(logger=get_logger("init_logger"))
    def __init__(self, e: events._Event):
        self.logger = module_logger
        data = e.get_task_data()
        self.id = data[Attr.unique_id]
        self.parent_id = data[Attr.parent_task_id]
        self.task_type = data[Attr.task_type]
        self.crt_ts = data[Attr.time]
        self._children = deque()

    def __repr__(self):
        return "{}(id={}, type={}, parent={}, children=({}))".format(
            self.__class__,
            self.id,
            self.task_type,
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
