"""Output format converters for concatenated functions."""

import functools
import inspect
from collections.abc import Callable, Sequence
from typing import Any, Unpack, cast, overload

from dags.annotations import get_annotations
from dags.exceptions import DagsError
from dags.signature import forwarder_annotations
from dags.typing import MixedTupleType, P, T


def _apply_return_annotation(
    wrapper: Callable[..., Any],
    func: Callable[..., Any],
    return_annotation: Any,
) -> None:
    """Apply a new return annotation to a wrapper function.

    The user-described view (parameters from `func`, plus the new
    `return_annotation`) is written to the wrapper's `__signature__`. The
    wrapper's `__annotations__` advertises the `*args, **kwargs` forwarder
    shape — the wrapper accepts anything at the Python level and only
    `func` expects the typed arguments. See `forwarder_annotations` for
    the rationale; `dags.get_annotations` recovers the user view from
    `__signature__`.

    `func` is itself typically a dags wrapper (e.g. the `with_signature`
    output of `concatenate_functions`), so its return annotation lives on
    `__signature__`, not `__annotations__` — read it via
    `dags.get_annotations`.
    """
    signature = inspect.signature(func)
    if "return" in get_annotations(func):
        signature = signature.replace(return_annotation=return_annotation)
    wrapper.__signature__ = signature  # ty: ignore[unresolved-attribute]
    wrapper.__annotations__ = forwarder_annotations()


def single_output(
    func: Callable[P, tuple[T, *Unpack[MixedTupleType]]] | Callable[P, tuple[T, ...]],
    *,
    set_annotations: bool = False,
) -> Callable[P, T]:
    """Convert tuple output to single output; i.e. the first element of the tuple."""

    @functools.wraps(func)
    def wrapper_single_output(*args: P.args, **kwargs: P.kwargs) -> T:
        raw = func(*args, **kwargs)
        return raw[0]

    if set_annotations:
        annotations = get_annotations(func)
        if "return" in annotations:
            # `func` is a tuple-returning concatenated function; its return
            # annotation (recovered from `__signature__`) is a tuple of type
            # strings, which `get_annotations`' `dict[str, str]` type cannot
            # express.
            tuple_of_types = cast("tuple[str, ...]", annotations["return"])
            _apply_return_annotation(wrapper_single_output, func, tuple_of_types[0])

    return wrapper_single_output


@overload
def dict_output(
    func: Callable[P, tuple[T, ...]],
    *,
    keys: Sequence[str],
    set_annotations: bool = False,
) -> Callable[P, dict[str, T]]: ...


@overload
def dict_output(
    *, keys: Sequence[str], set_annotations: bool = False
) -> Callable[[Callable[P, tuple[T, ...]]], Callable[P, dict[str, T]]]: ...


def dict_output(
    func: Callable[P, tuple[T, ...]] | None = None,
    *,
    keys: Sequence[str] | None = None,
    set_annotations: bool = False,
) -> (
    Callable[P, dict[str, T]]
    | Callable[[Callable[P, tuple[T, ...]]], Callable[P, dict[str, T]]]
):
    """Convert tuple output to dict output."""
    if keys is None:
        raise DagsError(
            "The 'keys' parameter is required for dict_output. Please provide a list "
            "of strings to be used as dictionary keys for the output values."
        )

    def decorator_dict_output(
        func: Callable[P, tuple[T, ...]],
    ) -> Callable[P, dict[str, T]]:
        @functools.wraps(func)
        def wrapper_dict_output(*args: P.args, **kwargs: P.kwargs) -> dict[str, T]:
            raw = func(*args, **kwargs)
            return dict(zip(keys, raw, strict=True))

        if set_annotations:
            annotations = get_annotations(func)
            if "return" in annotations:
                tuple_of_types = cast("tuple[str, ...]", annotations["return"])
                return_annotation = dict(zip(keys, tuple_of_types, strict=True))
                _apply_return_annotation(wrapper_dict_output, func, return_annotation)

        return wrapper_dict_output

    if callable(func):
        return decorator_dict_output(func)
    return decorator_dict_output


def list_output(
    func: Callable[P, tuple[T, ...]], *, set_annotations: bool = False
) -> Callable[P, list[T]]:
    """Convert tuple output to list output."""

    @functools.wraps(func)
    def wrapper_list_output(*args: P.args, **kwargs: P.kwargs) -> list[T]:
        raw = func(*args, **kwargs)
        return list(raw)

    if set_annotations:
        annotations = get_annotations(func)
        if "return" in annotations:
            tuple_of_types = cast("tuple[str, ...]", annotations["return"])
            _apply_return_annotation(wrapper_list_output, func, list(tuple_of_types))

    return wrapper_list_output


@overload
def aggregated_output(
    func: Callable[P, tuple[T, ...]],
    *,
    aggregator: Callable[[T, T], T],
    set_annotations: bool = False,
    return_annotation: str | None = None,
) -> Callable[P, T]: ...


@overload
def aggregated_output(
    *,
    aggregator: Callable[[T, T], T],
    set_annotations: bool = False,
    return_annotation: str | None = None,
) -> Callable[[Callable[P, tuple[T, ...]]], Callable[P, T]]: ...


def aggregated_output(
    func: Callable[P, tuple[T, ...]] | None = None,
    *,
    aggregator: Callable[[T, T], T] | None = None,
    set_annotations: bool = False,
    return_annotation: str | None = None,
) -> Callable[P, T] | Callable[[Callable[P, tuple[T, ...]]], Callable[P, T]]:
    """Aggregate tuple output.

    Args:
        func: The function to wrap. If provided, the decorator is applied immediately.
        aggregator: Binary reduction function to combine the output values.
        set_annotations: If True, set the return annotation on the wrapper function.
        return_annotation: The return type annotation to use. Only used when
            set_annotations is True.

    """
    if aggregator is None:
        raise DagsError(
            "The 'aggregator' parameter is required for aggregated_output. Please "
            "provide a function (e.g., sum, max, or a custom function) that will be "
            "used to combine the output values."
        )

    def decorator_aggregated_output(func: Callable[P, tuple[T, ...]]) -> Callable[P, T]:
        @functools.wraps(func)
        def wrapper_aggregated_output(*args: P.args, **kwargs: P.kwargs) -> T:
            raw = func(*args, **kwargs)
            agg = raw[0]
            for entry in raw[1:]:
                agg = aggregator(agg, entry)  # aggregator is assumed not None
            return agg

        if set_annotations and return_annotation is not None:
            _apply_return_annotation(wrapper_aggregated_output, func, return_annotation)

        return wrapper_aggregated_output

    if callable(func):
        return decorator_aggregated_output(func)
    return decorator_aggregated_output
