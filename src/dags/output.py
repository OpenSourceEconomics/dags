import functools
from collections.abc import Callable
from typing import overload

from dags.typing import P, T


def single_output(func: Callable[P, tuple[T, ...]]) -> Callable[P, T]:
    """Convert tuple output to single output; i.e. the first element of the tuple."""

    @functools.wraps(func)
    def wrapper_single_output(*args: P.args, **kwargs: P.kwargs) -> T:
        raw = func(*args, **kwargs)
        return raw[0]

    return wrapper_single_output


@overload
def dict_output(
    func: Callable[P, tuple[T, ...]], *, keys: list[str]
) -> Callable[P, dict[str, T]]: ...


@overload
def dict_output(
    *, keys: list[str]
) -> Callable[[Callable[P, tuple[T, ...]]], Callable[P, dict[str, T]]]: ...


def dict_output(
    func: Callable[P, tuple[T, ...]] | None = None, *, keys: list[str] | None = None
) -> (
    Callable[P, dict[str, T]]
    | Callable[[Callable[P, tuple[T, ...]]], Callable[P, dict[str, T]]]
):
    """Convert tuple output to dict output."""
    if keys is None:
        raise ValueError("keys is required")

    def decorator_dict_output(
        func: Callable[P, tuple[T, ...]],
    ) -> Callable[P, dict[str, T]]:
        @functools.wraps(func)
        def wrapper_dict_output(*args: P.args, **kwargs: P.kwargs) -> dict[str, T]:
            raw = func(*args, **kwargs)
            return dict(zip(keys, raw, strict=True))

        return wrapper_dict_output

    if callable(func):
        return decorator_dict_output(func)
    return decorator_dict_output


def list_output(func: Callable[P, tuple[T, ...]]) -> Callable[P, list[T]]:
    """Convert tuple output to list output."""

    @functools.wraps(func)
    def wrapper_list_output(*args: P.args, **kwargs: P.kwargs) -> list[T]:
        raw = func(*args, **kwargs)
        return list(raw)

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
        raise ValueError("aggregator is required")

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
