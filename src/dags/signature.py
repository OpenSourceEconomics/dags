import functools
import inspect
from collections.abc import Callable


def create_signature(
    args: list[str] | None = None, kwargs: list[str] | None = None
) -> inspect.Signature:
    """Create a inspect.Signature object based on args and kwargs.

    Args:
        args: The names of positional or keyword arguments.
        kwargs: The keyword only arguments.

    Returns
    -------
        The signature

    """
    args = [] if args is None else args
    kwargs = {} if kwargs is None else kwargs

    parameter_objects = []
    for arg in args:
        param = inspect.Parameter(
            name=arg,
            kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
        parameter_objects.append(param)

    for arg in kwargs:
        param = inspect.Parameter(
            name=arg,
            kind=inspect.Parameter.KEYWORD_ONLY,
        )
        parameter_objects.append(param)

    return inspect.Signature(parameters=parameter_objects)


def with_signature(
    func: Callable | None = None,
    *,
    args: list[str] | None = None,
    kwargs: list[str] | None = None,
    enforce: bool = True,
) -> Callable:
    """Decorator that adds a signature to a function of type `f(*args, **kwargs)`.

    Caveats: The created signature only contains the names of arguments and whether
    they are keyword only. There is no way of setting default values, type hints
    or other things.

    Args:
        func: The function to be decorated. Should take `*args`
            and `**kwargs` as only arguments.
        args: The names of positional or keyword arguments.
        kwargs: The keyword only arguments.
        enforce: Whether the signature should be enforced or just
            added to the function for introspection. This creates runtime
            overhead.

    Returns
    -------
        function: The function with signature.
    """

    def decorator_with_signature(func: Callable) -> Callable:
        _args: list[str] = [] if args is None else args
        _kwargs: list[str] = [] if kwargs is None else kwargs
        signature = create_signature(_args, _kwargs)

        if enforce:
            valid_kwargs: set[str] = set(_kwargs) | set(_args)
            funcname: str = getattr(func, "__name__", "function")

            @functools.wraps(func)
            def wrapper_with_signature(*args: object, **kwargs: object) -> object:
                _fail_if_too_many_positional_arguments(args, _args, funcname)
                present_args: set[str] = set(_args[: len(args)])
                present_kwargs: set[str] = set(kwargs)
                _fail_if_duplicated_arguments(present_args, present_kwargs, funcname)
                _fail_if_invalid_keyword_arguments(
                    present_kwargs,
                    valid_kwargs,
                    funcname,
                )
                return func(*args, **kwargs)
        else:

            def wrapper_with_signature(*args: object, **kwargs: object) -> object:
                return func(*args, **kwargs)

        wrapper_with_signature.__signature__ = signature

        return wrapper_with_signature

    if callable(func):
        return decorator_with_signature(func)
    return decorator_with_signature


def _fail_if_too_many_positional_arguments(
    present_args: tuple[str], argnames: list[str], funcname: str
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


def rename_arguments(
    func: Callable | None = None, *, mapper: dict[str, str] | None = None
) -> Callable:
    """Rename positional and keyword arguments of func.

    Args:
        func (callable): The function of which the arguments are renamed.
        mapper (dict): Dict of strings where keys are old names and values are new
            of arguments.

    Returns
    -------
        function: The function with renamed arguments.
    """

    def decorator_rename_arguments(
        func: Callable,
    ) -> Callable:
        old_parameters: dict[str, inspect.Parameter] = dict(
            inspect.signature(func).parameters
        )
        parameters: list[inspect.Parameter] = []
        # mapper is assumed not to be None when renaming is desired.
        for name, param in old_parameters.items():
            if mapper is not None and name in mapper:
                parameters.append(param.replace(name=mapper[name]))
            else:
                parameters.append(param)

        signature = inspect.Signature(parameters=parameters)

        reverse_mapper: dict[str, str] = (
            {v: k for k, v in mapper.items()} if mapper is not None else {}
        )

        @functools.wraps(func)
        def wrapper_rename_arguments(*args: object, **kwargs: object) -> object:
            internal_kwargs: dict[str, object] = {}
            for name, value in kwargs.items():
                if name in reverse_mapper:
                    internal_kwargs[reverse_mapper[name]] = value
                elif mapper is None or name not in mapper:
                    internal_kwargs[name] = value
            return func(*args, **internal_kwargs)

        wrapper_rename_arguments.__signature__ = signature

        # Preserve function type
        if isinstance(func, functools.partial):
            out: Callable = functools.partial(
                wrapper_rename_arguments,
                *func.args,
                **func.keywords,
            )
        else:
            out = wrapper_rename_arguments

        return out

    if callable(func):
        return decorator_rename_arguments(func)
    return decorator_rename_arguments
