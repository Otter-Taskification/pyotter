from abc import ABC, abstractmethod
from collections import defaultdict
import typing
from .. import definitions as defn


class TaskRegistrationError(KeyError):
    pass


class Task:
    """Represents an instance of a task"""

    def __init__(self, event):
        self.unique_id = event.unique_id
        self.task_type = event.task_type

    def __repr__(self):
        return f"{self.__class__}(id={self.unique_id}, type={self.task_type})"


class TaskRegistry:
    """
    Maintains references to all tasks encountered in a trace
    Maps task ID to task instance, raising KeyError if an unregistered task is requested
    """

    def __init__(self, *args, **attr):
        self._dict = dict()

    def __getitem__(self, uid: int) -> typing.Union[Task, None]:
        if uid not in self._dict:
            raise TaskRegistrationError(f"task {uid} was not found in {self}")
        return self._dict[uid]

    # def __setitem__(self, uid: int, event: events.Event) -> None:
    #     if uid in self._dict:
    #         raise KeyError(f"task {uid} was already registered in {self}")
    #     self._dict[uid] = Task(uid, event)

    def __iter__(self):
        for task in self._dict.values():
            yield task

    def __len__(self):
        return len(self._dict)

    def __repr__(self):
        return f"{self.__class__.__name__}({len(self._dict.keys())} tasks)"

    def register_task(self, e) -> Task:
        if e.unique_id in self._dict:
            raise TaskRegistrationError(f"task {e.unique_id} was already registered in {self}")
        self._dict[e.unique_id] = Task(e)
        print(f">>> REGISTERED {self._dict[e.unique_id]}")
        return self._dict[e.unique_id]

