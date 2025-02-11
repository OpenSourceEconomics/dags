import functools


def single_output(func):
    """Convert tuple output to single output."""

    @functools.wraps(func)
    def wrapper_single_output(*args, **kwargs):
        raw = func(*args, **kwargs)
        return raw[0]

    return wrapper_single_output


def dict_output(func=None, *, keys=None):
    """Convert tuple output to dict output."""

    def decorator_dict_output(func):
        @functools.wraps(func)
        def wrapper_dict_output(*args, **kwargs):
            raw = func(*args, **kwargs)
            return dict(zip(keys, raw))

        return wrapper_dict_output

    if callable(func):
        return decorator_dict_output(func)
    return decorator_dict_output


def list_output(func):
    """Convert tuple output to list output."""

    @functools.wraps(func)
    def wrapper_list_output(*args, **kwargs):
        raw = func(*args, **kwargs)
        return list(raw)

    return wrapper_list_output


def aggregated_output(func=None, *, aggregator=None):
    """Aggregate tuple output."""

    def decorator_aggregated_output(func):
        @functools.wraps(func)
        def wrapper_aggregated_output(*args, **kwargs):
            raw = func(*args, **kwargs)
            agg = raw[0]
            for entry in raw[1:]:
                agg = aggregator(agg, entry)
            return agg

        return wrapper_aggregated_output

    if callable(func):
        return decorator_aggregated_output(func)
    return decorator_aggregated_output
