import inspect
from collections.abc import Callable
from functools import partial
from typing import Any

import pytest

from dags.output import aggregated_output, dict_output, list_output, single_output
from dags.signature import with_signature

decorators = [
    single_output,
    list_output,
    partial(dict_output, keys=["a", "b"]),
    partial(aggregated_output, aggregator=lambda a, b: a + b),
]


@pytest.mark.parametrize("decorator", decorators)
def test_output_decorators_preserve_signature_created_by_with_signature(
    decorator: Callable[[Callable[..., Any]], Callable[..., Any]],
) -> None:
    @with_signature(args=["a"], kwargs=["b"])
    def f(*args, **kwargs):
        return (args[0], kwargs["b"])

    before = inspect.signature(f)
    g = decorator(f)
    after = inspect.signature(g)
    assert before == after
