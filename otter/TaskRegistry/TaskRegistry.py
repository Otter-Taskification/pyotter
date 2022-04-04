from typing import Union
from .. import log
from ..log import get_logger
from ..log.levels import DEBUG, INFO, WARN, ERROR
from ..EventFactory import events
from .tasks import Task
from ..definitions import Attr
from loggingdecorators import on_init
from warnings import warn

module_logger = get_logger("tasks")

class TaskRegistry:
    """
    Maintains references to all tasks encountered in a trace
    Maps task ID to task instance, raising KeyError if an unregistered task is requested
    """

    @on_init(logger=get_logger("init_logger"))
    def __init__(self):
        self.log = module_logger
        self._dict = dict()

    def __getitem__(self, uid: int) -> Union[Task, None]:
        if uid not in self._dict:
            raise KeyError(f"task {uid} was not found in {self}")
        return self._dict[uid]

    def __iter__(self):
        for task in self._dict.values():
            yield task

    def __len__(self):
        return len(self._dict)

    def __repr__(self):
        return f"{self.__class__.__name__}({len(self._dict.keys())} tasks)"

    def register_task(self, e: events._Event) -> Task:
        t = Task(e)
        self.log.debug(f"registering task {t.id} (parent={t.parent_id if t.id>0 else None})")
        if t.id in self._dict:
            raise ValueError(f"task {t.id} was already registered in {self}")
        self._dict[t.id] = t
        if t.id > 0:
            self[t.parent_id].append_child(t.id)
        return t

    def update_task(self, e: events._Event) -> Task:
        """Update the task data"""
        new_data = e.get_task_data()
        warn("This method is not finished!")
        return self[e.encountering_task_id]
