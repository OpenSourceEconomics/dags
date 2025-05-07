import functools
import inspect
from collections.abc import Callable
from typing import Any, cast, overload

from dags.typing import P, R


def create_signature(
    args: dict[str, type] | list[str] | None = None,
    kwargs: dict[str, type] | list[str] | None = None,
    return_annotation: type = inspect.Parameter.empty,
) -> inspect.Signature:
    """Create a inspect.Signature object based on args and kwargs.

    Args:
        args: If a list, the names of positional or keyword arguments. If a dict,
            the names of positional or keyword arguments and their types.
        kwargs: If a list, the names of keyword only arguments. If a dict,
            the names of keyword only arguments and their types.
        return_annotation: The return annotation.

    Returns
    -------
        The signature

    """
    parameter_objects = []
    for arg, arg_type in _map_names_to_types(args).items():
        param = inspect.Parameter(
            name=arg,
            kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
            annotation=arg_type,
        )
        parameter_objects.append(param)

    for kwarg, kwarg_type in _map_names_to_types(kwargs).items():
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
    args: dict[str, type] | list[str] | None = None,
    kwargs: dict[str, type] | list[str] | None = None,
    enforce: bool = True,
    return_annotation: type = inspect.Parameter.empty,
) -> Callable[P, R]: ...


@overload
def with_signature(
    *,
    args: dict[str, type] | list[str] | None = None,
    kwargs: dict[str, type] | list[str] | None = None,
    enforce: bool = True,
    return_annotation: type = inspect.Parameter.empty,
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...


def with_signature(
    func: Callable[P, R] | None = None,
    *,
    args: dict[str, type] | list[str] | None = None,
    kwargs: dict[str, type] | list[str] | None = None,
    enforce: bool = True,
    return_annotation: type = inspect.Parameter.empty,
) -> Callable[P, R] | Callable[[Callable[P, R]], Callable[P, R]]:
    """Add a signature to a function of type `f(*args, **kwargs)` (decorator).

    Caveats: The created signature only contains the names of arguments and whether
    they are keyword-only. There is no way of setting default values or type hints.

    Args:
        func: The function to be decorated. Should take `*args`
            and `**kwargs` as only arguments.
        args: If a list, the names of positional or keyword arguments. If a dict,
            the names of positional or keyword arguments and their types.
        kwargs: If a list, the names of keyword only arguments. If a dict,
            the names of keyword only arguments and their types.
        enforce: Whether the signature should be enforced or just
            added to the function for introspection. This creates runtime
            overhead.
        return_annotation: The return annotation.

    Returns
    -------
        function: The function with signature.
    """

    def decorator_with_signature(func: Callable[P, R]) -> Callable[P, R]:
        _args = _map_names_to_types(args)
        _kwargs = _map_names_to_types(kwargs)
        signature = create_signature(
            _args, _kwargs, return_annotation=return_annotation
        )
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
            return func(*args, **kwargs)

        wrapper_with_signature.__signature__ = signature  # type: ignore[attr-defined]
        return wrapper_with_signature

    if func is not None:
        return decorator_with_signature(func)
    return decorator_with_signature


def _fail_if_too_many_positional_arguments(
    present_args: tuple[Any, ...], argnames: list[str], funcname: str
) -> None:
    if len(present_args) > len(argnames):
        msg = (
            f"{funcname}() takes {len(argnames)} positional arguments "
            f"but {len(present_args)} were given"
        )
        raise TypeError(msg)


def _fail_if_duplicated_arguments(
    present_args: set[str], present_kwargs: set[str], funcname: str
) -> None:
    problematic = present_args & present_kwargs
    if problematic:
        s = "s" if len(problematic) >= 2 else ""
        problem_str = ", ".join(list(problematic))
        msg = f"{funcname}() got multiple values for argument{s} {problem_str}"
        raise TypeError(msg)


def _fail_if_invalid_keyword_arguments(
    present_kwargs: set[str], valid_kwargs: set[str], funcname: str
) -> None:
    problematic = present_kwargs - valid_kwargs
    if problematic:
        s = "s" if len(problematic) >= 2 else ""
        problem_str = ", ".join(list(problematic))
        msg = f"{funcname}() got unexpected keyword argument{s} {problem_str}"
        raise TypeError(msg)


@overload
def rename_arguments(
    func: Callable[P, R],
    *,
    mapper: dict[str, str],
) -> Callable[P, R]: ...


@overload
def rename_arguments(
    *, mapper: dict[str, str]
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...


def rename_arguments(
    func: Callable[P, R] | None = None, *, mapper: dict[str, str] | None = None
) -> Callable[P, R] | Callable[[Callable[P, R]], Callable[P, R]]:
    """Rename positional and keyword arguments of func.

    Args:
        func (callable): The function of which the arguments are renamed.
        mapper (dict): Dict of strings where keys are old names and values are new
            of arguments.

    Returns
    -------
        function: The function with renamed arguments.
    """

    def decorator_rename_arguments(func: Callable[P, R]) -> Callable[P, R]:
        old_signature = inspect.signature(func)
        old_parameters: dict[str, inspect.Parameter] = dict(old_signature.parameters)
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

        wrapper_rename_arguments.__signature__ = signature  # type: ignore[attr-defined]

        # Preserve function type
        if isinstance(func, functools.partial):
            partial_wrapper = functools.partial(
                wrapper_rename_arguments, *func.args, **func.keywords
            )
            out = cast("Callable[P, R]", partial_wrapper)
        else:
            out = wrapper_rename_arguments

        return out

    if func is not None:
        return decorator_rename_arguments(func)
    return decorator_rename_arguments


def _map_names_to_types(
    arg: dict[str, type] | list[str] | None,
) -> dict[str, type]:
    if arg is None:
        return {}
    if isinstance(arg, list):
        return dict.fromkeys(arg, inspect.Parameter.empty)
    if isinstance(arg, dict):
        return arg
    raise ValueError(f"Invalid type for arg: {type(arg)}")
