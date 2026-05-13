"""Utilities for extracting and verifying function annotations."""

import functools
import inspect
from collections.abc import Callable
from typing import Any, Literal, overload


def get_free_arguments(
    func: Callable[..., Any],
) -> list[str]:
    """Get the names of all free (non-partialled) arguments of a function.

    For regular functions, this returns all parameter names. For partial functions,
    arguments that have been bound via keywords are excluded.

    Args:
        func: The function to inspect.

    Returns:
    -------
        A list of argument names that are not bound.

    """
    arguments = list(inspect.signature(func).parameters)
    if isinstance(func, functools.partial):
        # arguments that are partialled by position are not part of the signature
        # anyways, so they do not need special handling.
        non_free = set(func.keywords)
        arguments = [arg for arg in arguments if arg not in non_free]

    return arguments


@overload
def get_annotations(
    func: Callable[..., Any],
    *,
    eval_str: Literal[False] = False,
    default: str | None = None,
) -> dict[str, str]: ...


@overload
def get_annotations(
    func: Callable[..., Any],
    *,
    eval_str: Literal[True] = True,
    default: type | None = None,
) -> dict[str, type]: ...


def get_annotations(
    func: Callable[..., Any],
    *,
    eval_str: bool = False,
    default: str | type | None = None,
) -> dict[str, str] | dict[str, type]:
    """Thin wrapper around inspect.get_annotations.

    Compared to inspect.get_annotations, this function also handles partialled funcs,
    and it returns annotations for all arguments, not just the ones with annotations.

    Args:
        func: The function to get annotations from.
        eval_str: If True, the string type annotations are evaluated.
        default: The default value to use if an annotation is missing. If None, the
            default value is inspect.Parameter.empty if eval_str is True, otherwise
            "no_annotation_found".

    Returns:
    -------
        A dictionary with the argument names as keys and the type annotations as values.
        The type annotations are strings if eval_str is False, otherwise they are types.

    """
    if default is None:
        default = inspect.Parameter.empty if eval_str else "no_annotation_found"

    if isinstance(func, functools.partial):
        annotations = inspect.get_annotations(func.func, eval_str=eval_str)
    else:
        annotations = inspect.get_annotations(func, eval_str=eval_str)

    free_arguments = get_free_arguments(func)
    annotation_keys = {k for k in annotations if k != "return"}
    signature_params = set(free_arguments)

    if _has_args_kwargs_annotation_mismatch(annotation_keys, signature_params):
        annotations = _get_annotations_from_signature(func, eval_str=eval_str)

    return {arg: annotations.get(arg, default) for arg in ["return", *free_arguments]}


def ensure_annotations_are_strings(annotations: dict[str, Any]) -> dict[str, str]:
    """Ensure all type annotations are strings, converting if necessary.

    In Python 3.14+, annotations may be evaluated at runtime rather than stored
    as strings. This function converts any non-string annotations to their string
    representation.

    Args:
        annotations: Dictionary of annotation names to their values.

    Returns:
    -------
        Dictionary with all annotation values as strings.

    """
    return {
        k: v if isinstance(v, str) else _get_str_repr(v) for k, v in annotations.items()
    }


def _get_str_repr(obj: object) -> str:
    return getattr(obj, "__name__", str(obj))


def _has_args_kwargs_annotation_mismatch(
    annotation_keys: set[str], signature_params: set[str]
) -> bool:
    """Check if annotations have the args/kwargs mismatch from Python 3.14.

    In Python 3.14, when functools.wraps wraps a non-function object (like
    PolicyFunction in ttsim) and the wrapper defines annotations with ParamSpec
    (*args: P.args, **kwargs: P.kwargs), functools.wraps no longer copies
    __annotations__ from the wrapped object; it uses the wrapper's annotations
    ({'args': 'P.args', 'kwargs': 'P.kwargs'}), which don't match the signature
    parameters that functools.wraps still copies correctly.

    """
    return annotation_keys != signature_params and annotation_keys == {"args", "kwargs"}


def _get_annotations_from_signature(
    func: Callable[..., Any], *, eval_str: bool
) -> dict[str, Any]:
    """Extract annotations from the function signature.

    This is a fallback for when inspect.get_annotations returns incorrect results:
    the Python 3.14 args/kwargs annotation mismatch, and dags wrappers
    (`with_signature`, `rename_arguments`, the `*_output` converters), which
    advertise the `*args, **kwargs` forwarder shape on `__annotations__` and
    keep the user-described view on `__signature__`.

    """
    sig = inspect.signature(func)
    annotations: dict[str, Any] = {}
    for param_name, param in sig.parameters.items():
        if param.annotation != inspect.Parameter.empty:
            annotations[param_name] = _normalise_annotation(
                param.annotation, eval_str=eval_str
            )
    if sig.return_annotation != inspect.Signature.empty:
        annotations["return"] = _normalise_annotation(
            sig.return_annotation, eval_str=eval_str
        )
    return annotations


def _normalise_annotation(annotation: Any, *, eval_str: bool) -> Any:
    """Normalise a single signature annotation for `get_annotations`.

    With `eval_str=True` the annotation is returned untouched. With
    `eval_str=False` the caller wants strings: bare `type` objects are
    reduced to their `__name__`, but strings and the structured return
    annotations produced by the `*_output` converters (a `tuple` / `list`
    / `dict` of type strings) are passed through as-is — stringifying
    those would lose their structure.
    """
    if eval_str or isinstance(annotation, (str, tuple, list, dict)):
        return annotation
    return _get_str_repr(annotation)
