"""Utilities for function signature manipulation."""

import functools
import inspect
from collections.abc import Callable, Mapping, Sequence
from typing import Any, overload

from dags.annotations import get_free_arguments
from dags.exceptions import DagsError, InvalidFunctionArgumentsError
from dags.typing import P, R


def _create_signature(
    args_types: dict[str, str] | dict[str, type[inspect._empty]],
    kwargs_types: dict[str, str] | dict[str, type[inspect._empty]],
    return_annotation: Any = inspect.Parameter.empty,
) -> inspect.Signature:
    """Create an inspect.Signature object based on args and kwargs.

    Args:
        args_types: The positional arguments mapped to their types as strings, or if no
            type is available, mapped to `inspect.Parameter.empty`.
        kwargs_types: The keyword arguments mapped to their types as strings, or if no
            type is available, mapped to `inspect.Parameter.empty`.
        return_annotation: The return annotation. By default, the return annotation is
            `inspect.Parameter.empty`.

    Returns:
    -------
        The signature.

    """
    parameter_objects = []
    for arg, arg_type in args_types.items():
        param = inspect.Parameter(
            name=arg,
            kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
            annotation=arg_type,
        )
        parameter_objects.append(param)

    for kwarg, kwarg_type in kwargs_types.items():
        param = inspect.Parameter(
            name=kwarg,
            kind=inspect.Parameter.KEYWORD_ONLY,
            annotation=kwarg_type,
        )
        parameter_objects.append(param)

    return inspect.Signature(
        parameters=parameter_objects, return_annotation=return_annotation
    )


@overload
def with_signature(
    func: Callable[P, R],
    *,
    args: Mapping[str, str] | Sequence[str] | None = None,
    kwargs: Mapping[str, str] | Sequence[str] | None = None,
    enforce: bool = True,
    return_annotation: Any = inspect.Parameter.empty,
) -> Callable[P, R]: ...


@overload
def with_signature(
    *,
    args: Mapping[str, str] | Sequence[str] | None = None,
    kwargs: Mapping[str, str] | Sequence[str] | None = None,
    enforce: bool = True,
    return_annotation: Any = inspect.Parameter.empty,
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...


def with_signature(
    func: Callable[P, R] | None = None,
    *,
    args: Mapping[str, str] | Sequence[str] | None = None,
    kwargs: Mapping[str, str] | Sequence[str] | None = None,
    enforce: bool = True,
    return_annotation: Any = inspect.Parameter.empty,
) -> Callable[P, R] | Callable[[Callable[P, R]], Callable[P, R]]:
    """Add a signature to a function of type `f(*args, **kwargs)` (decorator).

    The user-described view (parameter names, kinds, and any type hints passed
    via the dict form of `args` / `kwargs` or via `return_annotation`) is written
    to the wrapper's `__signature__`. The wrapper's `__annotations__` advertises
    the `*args, **kwargs` forwarder shape instead — the wrapper genuinely
    accepts anything at the Python level, and only the wrapped function
    expects the user-typed arguments. Runtime type checkers (beartype,
    typeguard) read `__annotations__` and therefore treat the wrapper as
    permissive; `dags.get_annotations` recovers the user view from
    `__signature__` via its built-in args/kwargs-mismatch fallback.

    The created signature carries argument names, whether each argument is
    keyword-only, and—when the dict form of `args`/`kwargs` is used or
    `return_annotation` is set—parameter and return type hints (written to
    `__signature__`, as described above). Default values cannot be set.

    Args:
        func: The function to be decorated. Should take `*args`
            and `**kwargs` as only arguments.
        args: If a list, the names of positional or keyword arguments. If a dict,
            the names of positional or keyword arguments mapped to their type hints
            (as strings).
        kwargs: If a list, the names of keyword only arguments. If a dict,
            the names of keyword only arguments mapped to their type hints (as
            strings).
        enforce: Whether the signature should be enforced or just
            added to the function for introspection. This creates runtime
            overhead.
        return_annotation: The return type hint. By default, the return annotation is
            `inspect.Parameter.empty`.

    Returns:
    -------
        function: The function with signature.
    """

    def decorator_with_signature(func: Callable[P, R]) -> Callable[P, R]:
        _args = _map_names_to_types(args)
        _kwargs = _map_names_to_types(kwargs)
        signature = _create_signature(_args, _kwargs, return_annotation)
        valid_kwargs: set[str] = set(_kwargs) | set(_args)
        funcname: str = getattr(func, "__name__", "function")

        @functools.wraps(func)
        def wrapper_with_signature(*args: P.args, **kwargs: P.kwargs) -> R:
            if enforce:
                _fail_if_too_many_positional_arguments(args, list(_args), funcname)
                present_args: set[str] = set(list(_args)[: len(args)])
                present_kwargs: set[str] = set(kwargs)
                _fail_if_duplicated_arguments(present_args, present_kwargs, funcname)
                _fail_if_invalid_keyword_arguments(
                    present_kwargs, valid_kwargs, funcname
                )
                _fail_if_missing_arguments(
                    present_args, present_kwargs, valid_kwargs, funcname
                )
            return func(*args, **kwargs)

        wrapper_with_signature.__signature__ = signature  # ty: ignore[unresolved-attribute]
        wrapper_with_signature.__annotations__ = forwarder_annotations()
        return wrapper_with_signature

    if func is not None:
        return decorator_with_signature(func)
    return decorator_with_signature


def forwarder_annotations() -> dict[str, Any]:
    """Build `__annotations__` advertising a `*args, **kwargs` forwarder.

    Every dags wrapper (`with_signature`, `rename_arguments`, the
    `*_output` converters) is a `def wrapper(*args, **kwargs)` forwarder:
    it accepts anything at the Python level and delegates to the wrapped
    function. Its `__annotations__` reflects that — `{"args": object,
    "kwargs": object}` — so runtime type checkers reading
    `__annotations__` (beartype, typeguard, `typing.get_type_hints`) see a
    permissive forwarder and do not enforce the wrapped function's
    per-parameter annotations against the wrapper's actual arguments.

    The user-described view (parameter names, types, return annotation)
    lives on the wrapper's `__signature__` instead, and
    `dags.get_annotations` recovers it from there via its
    args/kwargs-mismatch fallback. No return annotation is written so
    there are no string forward refs to resolve against the wrapper's
    `__module__`, where the referenced names may not be importable.
    """
    return {"args": object, "kwargs": object}


def _fail_if_too_many_positional_arguments(
    present_args: tuple[Any, ...], argnames: list[str], funcname: str
) -> None:
    if len(present_args) > len(argnames):
        msg = (
            f"{funcname} takes {len(argnames)} positional arguments "
            f"but {len(present_args)} were given"
        )
        raise InvalidFunctionArgumentsError(msg)


def _fail_if_duplicated_arguments(
    present_args: set[str], present_kwargs: set[str], funcname: str
) -> None:
    problematic = present_args & present_kwargs
    if problematic:
        s = "s" if len(problematic) >= 2 else ""  # noqa: PLR2004
        problem_str = ", ".join(list(problematic))
        msg = f"{funcname} got multiple values for argument{s} {problem_str}"
        raise InvalidFunctionArgumentsError(msg)


def _fail_if_invalid_keyword_arguments(
    present_kwargs: set[str], valid_kwargs: set[str], funcname: str
) -> None:
    problematic = present_kwargs - valid_kwargs
    if problematic:
        s = "s" if len(problematic) >= 2 else ""  # noqa: PLR2004
        problem_str = ", ".join(list(problematic))
        msg = f"{funcname} got unexpected keyword argument{s} {problem_str}"
        raise InvalidFunctionArgumentsError(msg)


def _fail_if_missing_arguments(
    present_args: set[str],
    present_kwargs: set[str],
    required_args: set[str],
    funcname: str,
) -> None:
    provided = present_args | present_kwargs
    missing = required_args - provided
    if missing:
        s = "s" if len(missing) >= 2 else ""  # noqa: PLR2004
        missing_str = ", ".join(sorted(missing))
        msg = f"{funcname} is missing required argument{s}: {missing_str}"
        raise InvalidFunctionArgumentsError(msg)


@overload
def rename_arguments(
    func: Callable[P, R],
    *,
    mapper: Mapping[str, str],
) -> Callable[..., R]: ...


@overload
def rename_arguments(
    *, mapper: Mapping[str, str]
) -> Callable[[Callable[P, R]], Callable[..., R]]: ...


def rename_arguments(
    func: Callable[P, R] | None = None,
    *,
    mapper: Mapping[str, str] | None = None,
) -> Callable[..., R] | Callable[[Callable[P, R]], Callable[..., R]]:
    """Rename positional and keyword arguments of func.

    The renamed user-described view is written to the wrapper's
    `__signature__`. The wrapper's `__annotations__` advertises the
    `*args, **kwargs` forwarder shape — runtime type checkers see a
    permissive forwarder, and `dags.get_annotations` recovers the renamed
    view from `__signature__` via its args/kwargs-mismatch fallback. See
    `forwarder_annotations` for the rationale.

    Args:
        func (callable): The function of which the arguments are renamed.
        mapper (dict): Dict of strings where keys are old names and values are new
            of arguments.

    Returns:
    -------
        function: The function with renamed arguments.
    """

    def decorator_rename_arguments(func: Callable[P, R]) -> Callable[..., R]:
        old_signature = inspect.signature(func)
        free_arguments = set(get_free_arguments(func))
        old_parameters: dict[str, inspect.Parameter] = {
            name: param
            for name, param in old_signature.parameters.items()
            if name in free_arguments
        }

        parameters: list[inspect.Parameter] = []
        # mapper is assumed not to be None when renaming is desired.
        for name, param in old_parameters.items():
            if mapper is not None and name in mapper:
                parameters.append(param.replace(name=mapper[name]))
            else:
                parameters.append(param)

        signature = inspect.Signature(
            parameters=parameters, return_annotation=old_signature.return_annotation
        )

        reverse_mapper: dict[str, str] = (
            {v: k for k, v in mapper.items()} if mapper is not None else {}
        )

        @functools.wraps(func)
        def wrapper_rename_arguments(*args: P.args, **kwargs: P.kwargs) -> R:
            internal_kwargs: dict[str, Any] = {}
            for name, value in kwargs.items():
                if name in reverse_mapper:
                    internal_kwargs[reverse_mapper[name]] = value
                elif mapper is None or name not in mapper:
                    internal_kwargs[name] = value
            return func(*args, **internal_kwargs)

        wrapper_rename_arguments.__signature__ = signature  # ty: ignore[unresolved-attribute]
        wrapper_rename_arguments.__annotations__ = forwarder_annotations()

        return wrapper_rename_arguments

    if func is not None:
        return decorator_rename_arguments(func)
    return decorator_rename_arguments


def _map_names_to_types(
    arg: Mapping[str, str] | Sequence[str] | None,
) -> dict[str, str] | dict[str, type[inspect._empty]]:
    if arg is None:
        return {}
    if isinstance(arg, Mapping):
        return dict(arg)
    if isinstance(arg, str):
        raise DagsError(
            f"Invalid type for arg: {type(arg)}. Expected Mapping, Sequence, or None."
        )
    if isinstance(arg, Sequence):
        return dict.fromkeys(arg, inspect.Parameter.empty)
    raise DagsError(
        f"Invalid type for arg: {type(arg)}. Expected Mapping, Sequence, or None."
    )
