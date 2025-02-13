import functools
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


def single_output(func: Callable) -> Callable:
    """Convert tuple output to single output."""

    @functools.wraps(func)
    def wrapper_single_output(*args: object, **kwargs: object) -> T:
        raw: tuple[T, ...] = func(*args, **kwargs)
        return raw[0]

    return wrapper_single_output


def dict_output(
    func: Callable | None = None, *, keys: list[str] | None = None
) -> Callable[..., dict[str, T]] | Callable[[Callable], Callable[..., dict[str, T]]]:
    """Convert tuple output to dict output."""

    def decorator_dict_output(
        func: Callable,
    ) -> Callable[..., dict[str, T]]:
        @functools.wraps(func)
        def wrapper_dict_output(*args: object, **kwargs: object) -> dict[str, T]:
            raw: tuple[T, ...] = func(*args, **kwargs)
            return dict(zip(keys, raw, strict=True))

        return wrapper_dict_output

    if callable(func):
        return decorator_dict_output(func)
    return decorator_dict_output


def list_output(func: Callable) -> Callable[..., list[T]]:
    """Convert tuple output to list output."""

    @functools.wraps(func)
    def wrapper_list_output(*args: object, **kwargs: object) -> list[T]:
        raw: tuple[T, ...] = func(*args, **kwargs)
        return list(raw)

    return wrapper_list_output


def aggregated_output(
    func: Callable | None = None,
    *,
    aggregator: Callable[[T, T], T] | None = None,
) -> Callable | Callable[[Callable], Callable]:
    """Aggregate tuple output."""

    def decorator_aggregated_output(
        func: Callable,
    ) -> Callable:
        @functools.wraps(func)
        def wrapper_aggregated_output(*args: object, **kwargs: object) -> T:
            raw: tuple[T, ...] = func(*args, **kwargs)
            agg: T = raw[0]
            for entry in raw[1:]:
                agg = aggregator(agg, entry)
            return agg

        return wrapper_aggregated_output

    if callable(func):
        return decorator_aggregated_output(func)
    return decorator_aggregated_output
