from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, overload

from dags.exceptions import NonStringAnnotationError

if TYPE_CHECKING:
    from collections.abc import Callable

import functools
import inspect


def get_free_arguments(
    func: Callable[..., Any],
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
    func: Callable[..., Any],
    eval_str: Literal[False] = False,
    default: str | None = None,
) -> dict[str, str]: ...


@overload
def get_annotations(
    func: Callable[..., Any],
    eval_str: Literal[True] = True,
    default: type | None = None,
) -> dict[str, type]: ...


def get_annotations(
    func: Callable[..., Any],
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

    Returns
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
        annotations = _get_annotations_from_signature(func, eval_str)

    return {arg: annotations.get(arg, default) for arg in ["return", *free_arguments]}


def verify_annotations_are_strings(
    annotations: dict[str, str], function_name: str
) -> None:
    # If all annotations are strings, we are done.
    if all(isinstance(v, str) for v in annotations.values()):
        return

    non_string_annotations = [
        k for k, v in annotations.items() if not isinstance(v, str)
    ]
    arg_annotations = {k: v for k, v in annotations.items() if k != "return"}
    return_annotation = annotations["return"]

    # Create a representation of the signature with string annotations
    # ----------------------------------------------------------------------------------
    stringified_arg_annotations = []
    for k, v in arg_annotations.items():
        if k in non_string_annotations:
            stringified_arg_annotations.append(f"{k}: '{_get_str_repr(v)}'")
        else:
            annot = f"{k}: '{v}'"
            stringified_arg_annotations.append(annot)

    if "return" in non_string_annotations:
        stringified_return_annotation = f"'{_get_str_repr(return_annotation)}'"
    else:
        stringified_return_annotation = f"'{return_annotation}'"

    stringified_signature = (
        f"{function_name}({', '.join(stringified_arg_annotations)}) -> "
        f"{stringified_return_annotation}"
    )

    # Create message on which argument and/or return annotation is invalid
    # ----------------------------------------------------------------------------------
    invalid_arg_annotations = [k for k in non_string_annotations if k != "return"]
    if invalid_arg_annotations:
        s = "s" if len(invalid_arg_annotations) > 1 else ""
        invalid_arg_msg = f"argument{s} ({', '.join(invalid_arg_annotations)})"
    else:
        invalid_arg_msg = ""

    invalid_annotations_msg = ""
    if invalid_arg_msg and "return" in non_string_annotations:
        invalid_annotations_msg = f"{invalid_arg_msg} and the return value"
    elif invalid_arg_msg:
        invalid_annotations_msg = invalid_arg_msg
    elif "return" in non_string_annotations:
        invalid_annotations_msg = "return value"

    raise NonStringAnnotationError(
        f"All function annotations must be strings. The annotations for the "
        f"{invalid_annotations_msg} are not strings.\nA simple way for Python to treat "
        "type annotations as strings is to add\n\n\tfrom __future__ import annotations"
        "\n\nat the top of your file. Alternatively, you can do it manually by "
        f"enclosing the annotations in quotes:\n\n\t{stringified_signature}."
    )


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
    func: Callable[..., Any], eval_str: bool
) -> dict[str, Any]:
    """Extract annotations from the function signature.

    This is a fallback for when inspect.get_annotations returns incorrect results,
    such as in Python 3.14's args/kwargs annotation mismatch case.

    """
    sig = inspect.signature(func)
    annotations: dict[str, Any] = {}
    for param_name, param in sig.parameters.items():
        if param.annotation != inspect.Parameter.empty:
            annotations[param_name] = (
                param.annotation
                if eval_str or isinstance(param.annotation, str)
                else _get_str_repr(param.annotation)
            )
    if sig.return_annotation != inspect.Signature.empty:
        annotations["return"] = (
            sig.return_annotation
            if eval_str or isinstance(sig.return_annotation, str)
            else _get_str_repr(sig.return_annotation)
        )
    return annotations
