from __future__ import annotations

import functools
import inspect
from typing import TYPE_CHECKING, get_args, overload

from typing_extensions import Unpack

from dags.exceptions import DagsError

if TYPE_CHECKING:
    from collections.abc import Callable

    from dags.typing import HetTupleType, P, T


def single_output(
    func: Callable[P, tuple[T, Unpack[HetTupleType]]] | Callable[P, tuple[T, ...]],
    set_annotations: bool = False,
) -> Callable[P, T]:
    """Convert tuple output to single output; i.e. the first element of the tuple."""

    @functools.wraps(func)
    def wrapper_single_output(*args: P.args, **kwargs: P.kwargs) -> T:
        raw = func(*args, **kwargs)
        return raw[0]

    if set_annotations:
        signature = inspect.signature(func)
        annotations = inspect.get_annotations(func, eval_str=True)

        if "return" in annotations:
            return_annotation = get_args(annotations["return"])[0]
            if isinstance(return_annotation, type):
                return_annotation_str = return_annotation.__name__
            else:
                return_annotation_str = return_annotation
            signature = signature.replace(return_annotation=return_annotation_str)
            annotations["return"] = return_annotation_str

        wrapper_single_output.__signature__ = signature  # type: ignore[attr-defined]
        wrapper_single_output.__annotations__ = annotations

    return wrapper_single_output


@overload
def dict_output(
    func: Callable[P, tuple[T, ...]], *, keys: list[str], set_annotations: bool
) -> Callable[P, dict[str, T]]: ...


@overload
def dict_output(
    *, keys: list[str], set_annotations: bool
) -> Callable[[Callable[P, tuple[T, ...]]], Callable[P, dict[str, T]]]: ...


def dict_output(
    func: Callable[P, tuple[T, ...]] | None = None,
    *,
    keys: list[str] | None = None,
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
            signature = inspect.signature(func)
            annotations = inspect.get_annotations(func, eval_str=True)
            if "return" in annotations:
                tuple_types = get_args(annotations["return"])
                tuple_types_str = [
                    et.__name__ if isinstance(et, type) else et for et in tuple_types
                ]
                dict_entries = [
                    f"'{k}': {v}" for k, v in zip(keys, tuple_types_str, strict=True)
                ]
                return_annotation = f"{{{', '.join(dict_entries)}}}"
                signature = signature.replace(return_annotation=return_annotation)
                annotations["return"] = return_annotation
            wrapper_dict_output.__signature__ = signature  # type: ignore[attr-defined]
            wrapper_dict_output.__annotations__ = annotations

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
        signature = inspect.signature(func)
        annotations = inspect.get_annotations(func, eval_str=True)
        if "return" in annotations:
            tuple_types = get_args(annotations["return"])
            tuple_types_str = [
                et.__name__ if isinstance(et, type) else et for et in tuple_types
            ]
            union_type = " | ".join(set(tuple_types_str))
            return_annotation = f"list[{union_type}]"
            signature = signature.replace(return_annotation=return_annotation)
            annotations["return"] = return_annotation

        wrapper_list_output.__signature__ = signature  # type: ignore[attr-defined]
        wrapper_list_output.__annotations__ = annotations

    return wrapper_list_output


@overload
def aggregated_output(
    func: Callable[P, tuple[T, ...]], *, aggregator: Callable[[T, T], T]
) -> Callable[P, T]: ...


@overload
def aggregated_output(
    *, aggregator: Callable[[T, T], T]
) -> Callable[[Callable[P, tuple[T, ...]]], Callable[P, T]]: ...


def aggregated_output(
    func: Callable[P, tuple[T, ...]] | None = None,
    *,
    aggregator: Callable[[T, T], T] | None = None,
) -> Callable[P, T] | Callable[[Callable[P, tuple[T, ...]]], Callable[P, T]]:
    """Aggregate tuple output."""
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

        return wrapper_aggregated_output

    if callable(func):
        return decorator_aggregated_output(func)
    return decorator_aggregated_output
