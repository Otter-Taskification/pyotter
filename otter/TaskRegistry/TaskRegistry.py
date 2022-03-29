from typing import Union
from ..EventFactory import events
from ..definitions import Attr
from ..utils import PrettyCounter
from .tasks import Task
from ..logging import get_logger
from .. import utils

class TaskRegistry:
    """
    Maintains references to all tasks encountered in a trace
    Maps task ID to task instance, raising KeyError if an unregistered task is requested
    """

    @utils.decorate.log_init
    def __init__(self):
        self.log = get_logger(f"{self.__class__.__name__}")
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
        self.log.debug(f"{self.__class__.__name__} registering task {t.id} (parent={t.parent_id if t.id>0 else None})")
        if t.id in self._dict:
            raise ValueError(f"task {t.id} was already registered in {self}")
        self._dict[t.id] = t
        if t.id > 0:
            self[t.parent_id].append_child(t.id)
        return t
