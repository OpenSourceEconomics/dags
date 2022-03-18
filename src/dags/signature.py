import functools
import inspect


def create_signature(args=None, kwargs=None):
    """Create a inspect.Signature object based on args and kwargs.

    Args:
        args (list or None): The names of positional or keyword arguments.
        kwargs (list or None): The keyword only arguments.

    Returns:
        inspect.Signature

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

    sig = inspect.Signature(parameters=parameter_objects)
    return sig


def with_signature(func=None, *, args=None, kwargs=None, enforce=True):
    """Decorator that adds a signature to a function of type ``f(*args, **kwargs)``

    Caveats: The created signature only contains the names of arguments and whether
    they are keyword only. There is no way of setting default values, type hints
    or other things.

    Args:
        func (callable): The function to be decorated. Should take ``*args``
            and ``**kwargs`` as only arguments.
        args (list or None): The names of positional or keyword arguments.
        kwargs (list or None): The keyword only arguments.
        enforce (bool): Whether the signature should be enforced or just
            added to the function for introspection. This creates runtime
            overhead.

    Returns:
        function: The function with signature.

    """

    def decorator_with_signature(func):
        _args = [] if args is None else args
        _kwargs = [] if kwargs is None else kwargs
        signature = create_signature(_args, _kwargs)

        if enforce:
            valid_kwargs = set(_kwargs) | set(_args)
            funcname = getattr(func, "__name__", "function")

            @functools.wraps(func)
            def wrapper_with_signature(*args, **kwargs):
                _fail_if_too_many_positional_arguments(args, _args, funcname)
                present_args = set(_args[: len(args)])
                present_kwargs = set(kwargs)
                _fail_if_duplicated_arguments(present_args, present_kwargs, funcname)
                _fail_if_invalid_keyword_arguments(
                    present_kwargs, valid_kwargs, funcname
                )
                return func(*args, **kwargs)

        else:

            def wrapper_with_signature(*args, **kwargs):
                return func(*args, **kwargs)

        wrapper_with_signature.__signature__ = signature

        return wrapper_with_signature

    if callable(func):
        return decorator_with_signature(func)
    else:
        return decorator_with_signature


def _fail_if_too_many_positional_arguments(present_args, argnames, funcname):
    if len(present_args) > len(argnames):
        raise TypeError(
            f"{funcname}() takes {len(argnames)} positional arguments "
            f"but {len(present_args)} were given"
        )


def _fail_if_duplicated_arguments(present_args, present_kwargs, funcname):
    problematic = present_args & present_kwargs
    if problematic:
        s = "s" if len(problematic) >= 2 else ""
        problem_str = ", ".join(list(problematic))
        raise TypeError(
            f"{funcname}() got multiple values for argument{s} {problem_str}"
        )


def _fail_if_invalid_keyword_arguments(present_kwargs, valid_kwargs, funcname):
    problematic = present_kwargs - valid_kwargs
    if problematic:
        s = "s" if len(problematic) >= 2 else ""
        problem_str = ", ".join(list(problematic))
        raise TypeError(
            f"{funcname}() got unexpected keyword argument{s} {problem_str}"
        )


def rename_arguments(func=None, *, mapper=None):
    """Rename positional and keyword arguments of func.

    Args:
        func (callable): The function of which the arguments are renamed.
        mapper (dict): Dict of strings where keys are old names and values are new
            of arguments.

    Returns:
        function: The function with renamed arguments.

    """

    def decorator_rename_arguments(func):

        old_parameters = dict(inspect.signature(func).parameters)
        parameters = []
        for name, param in old_parameters.items():
            if name in mapper:
                parameters.append(param.replace(name=mapper[name]))
            else:
                parameters.append(param)

        signature = inspect.Signature(parameters=parameters)

        reverse_mapper = {v: k for k, v in mapper.items()}

        @functools.wraps(func)
        def wrapper_rename_arguments(*args, **kwargs):
            internal_kwargs = {}
            for name, value in kwargs.items():
                if name in reverse_mapper:
                    internal_kwargs[reverse_mapper[name]] = value
                elif name not in mapper:
                    internal_kwargs[name] = value
            return func(*args, **internal_kwargs)

        wrapper_rename_arguments.__signature__ = signature

        return wrapper_rename_arguments

    if callable(func):
        return decorator_rename_arguments(func)
    else:
        return decorator_rename_arguments
