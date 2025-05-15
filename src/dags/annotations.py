from __future__ import annotations

from typing import TYPE_CHECKING, Literal, overload

if TYPE_CHECKING:
    from dags.typing import GenericCallable

import functools
import inspect


def get_free_arguments(
    func: GenericCallable,
) -> list[str]:
    arguments = list(inspect.signature(func).parameters)
    if isinstance(func, functools.partial):
        # arguments that are partialled by position are not part of the signature
        # anyways, so they do not need special handling.
        non_free = set(func.keywords)
        arguments = [arg for arg in arguments if arg not in non_free]

    return arguments


@overload
def get_annotations(
    func: GenericCallable,
    eval_str: Literal[False] = False,
    default: str | None = None,
) -> dict[str, str]: ...


@overload
def get_annotations(
    func: GenericCallable,
    eval_str: Literal[True] = True,
    default: type | None = None,
) -> dict[str, type]: ...


def get_annotations(
    func: GenericCallable,
    eval_str: bool = False,
    default: str | type | None = None,
) -> dict[str, str] | dict[str, type]:
    """Thin wrapper around inspect.get_annotations to also handle partialled funcs.

    Args:
        func: The function to get annotations from.
        eval_str: If True, the string type annotations are evaluated.
        default: The default value to use if an annotation is missing. If None, the
            default value is inspect.Parameter.empty if eval_str is True, otherwise
            "unknown_type".

    Returns
    -------
        A dictionary with the argument names as keys and the type annotations as values.
        The type annotations are strings if eval_str is False, otherwise they are types.

    Raises
    ------
        NonStringAnnotationError: If the type annotations are not strings.

    """
    if default is None:
        default = inspect.Parameter.empty if eval_str else "unknown_type"

    if isinstance(func, functools.partial):
        annotations = inspect.get_annotations(func.func, eval_str=eval_str)
    else:
        annotations = inspect.get_annotations(func, eval_str=eval_str)
    free_arguments = get_free_arguments(func)
    return {arg: annotations.get(arg, default) for arg in ["return", *free_arguments]}
