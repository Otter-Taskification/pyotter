from collections import deque
from ..EventFactory import events
from ..definitions import Attr
from ..Logging import get_logger


class Task:
    """Represents an instance of a task"""

    def __init__(self, e: events._Event):
        self.log = get_logger(f"{self.__class__.__name__}")
        self.log.debug(f"constructing task for event {e}")
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
