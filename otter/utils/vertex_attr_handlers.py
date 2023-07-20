# TODO: re-implement any handlers with event model logic inside an event model

# TODO: ideas which need to be factored out of this module:
#  reduction operations for Iterable[V] -> V
#  wrapping reductions with log messages
#  validating reduction arguments
#  ==> these 3 operations should be separately composable for flexibility

from typing import Type, List, Iterable, Union, Protocol, TypeVar, Optional, Any
from .. import log
from ..log import DEBUG
from ..definitions import RegionType
from ..core import events
from ..utils import flatten
from .decorators import warn_deprecated
from loggingdecorators import on_init, on_call

get_module_logger = log.logger_getter("vertex.attr")

V = TypeVar("V")


class Reduction(Protocol[V]):
    """
    A callable which reduces an Iterable of Optional[V] to an Optional[V]
    """

    def __call__(self, collection: Iterable[Optional[V]]) -> Optional[V]:
        ...


class ReductionDict(dict):
    """
    Maps strings to reduction operations
    """

    @on_init(logger=log.logger_getter("init_logger"))
    def __init__(
        self,
        keys: Iterable[str],
        default_reduction: Reduction = None,
        logger=None,
        level=DEBUG,
    ):
        self.log = logger or log.get_logger(self.__class__.__name__)
        self.level = level
        reduction: Reduction = default_reduction or pass_first_arg
        super().__init__({key: reduction for key in keys})

    def __setitem__(self, key: str, value: Reduction):
        self.log.log(
            self.level, f"set reduction '{value}' for string '{key}'", stacklevel=2
        )
        return super().__setitem__(key, value)


# TODO: this class does too many things - decorates the handler to call, validates reduction arguments, applies handler
class LoggingValidatingReduction:
    """
    Apply a strategy for combining a set of values of some attribute
    Apply a given reduction operation to an iterable of values. Decorates the given reduction to log each time it is called.
    Validates the arguments to the reduction before applying the reduction.
    """

    @on_init(logger=log.logger_getter("init_logger"))
    def __init__(
        self,
        reduction: Reduction = None,
        accept: Union[Type, List[Type]] = None,
        msg="combining events",
    ):
        """
        Wraps a handler function to allow checking and logging of the arguments passed to it.
        Log invocations of the handler with an on_call decorator.

        handler: a function which accepts one arg (a list of the values to be combined)
        accept: a type, or list of types, which each arg must conform to for the given handler to be applied
        """

        # TODO: if reduction is None, this doesn't actually perform a reduction - is that intentional?
        self.reduction = reduction if reduction else (lambda arg: arg)

        self.accept = accept or [list, events.Event]
        self.log = log.get_logger(self.__class__.__name__)

        # TODO: don't bake decorated reduction in, lift out and allow to be injected
        call_decorator = on_call(self.log, msg=msg)
        self.reduction = call_decorator(self.reduction)

    def __call__(self, args: List[Optional[V]]) -> Optional[V]:
        """
        Called by igraph.Graph.contract_vertices when combining a vertex attribute
        Expects args to be a list of values to be combined
        Returns the value to assign to the attribute of the vertex which replaces the contracted vertices
        If args is a list of lists, flatten into one list before passing to handler
        If args is a list with 1 item, return that item
        """

        assert isinstance(args, list)

        # Prevent any nesting of lists when args is a List[List[events._Event]]
        args = list(flatten(args))

        # TODO: this assertion should really be a unit test of flatten when called with nested lists
        assert isinstance(args, list) and all(
            not isinstance(item, list) for item in args
        )  # flat list

        if all(arg is None for arg in args):
            self.log.debug(f"all args were None", stacklevel=4)
            return None

        # Filter out all values which are None (allow False to pass through)
        args = [arg for arg in args if arg is not None]

        if len(args) == 1:
            item = args[0]
            self.log.debug(f"list contains 1 item: {item}", stacklevel=4)
            # TODO: remove _Event api call and encapsulate requirement for events to always be inside a list
            return [item] if events.is_event(item) else item

        # Each arg must be of the given type (or one of the given list of types) for this handler
        A = isinstance(self.accept, Iterable) and all(
            isinstance(arg, tuple(self.accept)) for arg in args
        )
        B = isinstance(self.accept, type) and are(all, args, self.accept)
        if not (A or B):
            raise TypeError(f"expected {self.accept}, got {set(map(type, args))}")

        # args is list with > 1 element, none of which is None
        return self.reduction(args)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.reduction.__module__}.{self.reduction.__name__})"


### Strategies for reducing lists of attribute values to one value


# TODO: this function is poorly named. Maybe instances_of?
# TODO: consider removing as only used once
def are(reduce: Reduction[bool], args: List[Any], cls) -> bool:
    return reduce(isinstance(x, cls) for x in args)


def drop_args(args: Iterable[Any]) -> None:
    return None


def pass_args(args: List[Optional[V]]) -> List[Optional[V]]:
    return args


def pass_first_arg(args: List[Optional[V]]) -> Optional[V]:
    return args[0]


# TODO: perhaps this handler is too specific to need defining here - consider defining in-line in application code.
# TODO: as it's only used in 1 place
def pass_bool_value(values: List[Optional[bool]]) -> None:
    """
    Given a list containing Optional[bool], where all boolean values are the same, return the single boolean value.
    """
    # TODO: An `assert` would be more explicit than this `if` since we raise if the check fails anyway.
    # assert set(values) in [{True, None}, {False, None}]
    # assert set(map(type, values)) == {bool, type(None)}
    if set(map(type, values)) == {bool, type(None)}:
        A, B = set(values)
        # return the non-None boolean
        return A if B is None else B
    raise NotImplementedError(f"not implemented for {values=}")


def pass_the_unique_value(args):
    items = set(args)
    if len(items) == 1:
        return items.pop()
    raise ValueError(f"expected one unique item: {items=}")


def pass_the_set_of_values(args):
    items = set(args)
    if len(items) == 1:
        return items.pop()
    else:
        return items
