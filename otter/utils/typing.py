from typing import Callable, TypeVar


CallableType = TypeVar("CallableType", bound=Callable)
Decorator = Callable[[CallableType], CallableType]
