"""Runtime type checkers must treat dags wrappers as permissive forwarders.

Every dags wrapper (`with_signature`, `rename_arguments`, the `*_output`
converters) is a `def wrapper(*args, **kwargs)` forwarder: it accepts anything at
the Python level and delegates to the wrapped function. Its `__annotations__`
advertises that forwarder shape (`{"args": object, "kwargs": object}`) while the
user-described view (parameter names, types, return annotation) lives on
`__signature__`.

This mirrors how dags is consumed downstream: pylcm installs beartype's import
claw on subpackages that build DAGs with dags wrappers. If a dags wrapper ever
advertised the user view on `__annotations__` again, beartype would enforce the
wrapped function's per-parameter types against the wrapper's actual arguments and
reject calls the wrapper legitimately forwards (e.g. JAX tracers passed where the
user annotation says `float`). These tests guard that invariant: beartype-
decorating a dags wrapper and calling it with arguments that violate the *user*
signature must not raise.

Do not add "from __future__ import annotations" here -- the test relies on the
wrappers' `__annotations__` being the actual forwarder objects, not strings.
"""

from collections.abc import Callable
from functools import partial
from typing import Any

import pytest
from beartype import BeartypeConf, BeartypeStrategy, beartype

from dags.dag import concatenate_functions
from dags.output import aggregated_output, dict_output, list_output, single_output
from dags.signature import rename_arguments, with_signature

# Representative of pylcm's perimeter setup: full O(n) container validation and
# the PEP-484 numeric tower. The forwarder-shape invariant is conf-independent,
# but exercising a realistic conf keeps the test honest against the way dags is
# actually consumed downstream.
_CONF = BeartypeConf(strategy=BeartypeStrategy.On, is_pep484_tower=True)


def _with_signature_wrapper() -> Callable[..., Any]:
    @with_signature(args={"a": "int"}, kwargs={"b": "float"})
    def f(*args: Any, **kwargs: Any) -> tuple[Any, Any]:
        return (args[0], kwargs["b"])

    return f


def _rename_arguments_wrapper() -> Callable[..., Any]:
    def base(x: int, *, y: float) -> tuple[int, float]:
        return (x, y)

    return rename_arguments(base, mapper={"x": "a", "y": "b"})


def _output_wrapper(
    converter: Callable[..., Callable[..., Any]],
) -> Callable[..., Any]:
    def f(*args: Any, **kwargs: Any) -> tuple[Any, Any]:
        return (args[0], kwargs["b"])

    # A tuple return annotation, set directly because Python has no tuple syntax
    # in annotation position. This is the shape the `*_output` converters consume
    # from `concatenate_functions`.
    f.__annotations__ = {"a": "int", "b": "float", "return": ("int", "float")}
    return converter(f, set_annotations=True)


_WRAPPER_FACTORIES: dict[str, Callable[[], Callable[..., Any]]] = {
    "with_signature": _with_signature_wrapper,
    "rename_arguments": _rename_arguments_wrapper,
    "single_output": partial(_output_wrapper, single_output),
    "list_output": partial(_output_wrapper, list_output),
    "dict_output": partial(_output_wrapper, partial(dict_output, keys=["a", "b"])),
    "aggregated_output": partial(
        _output_wrapper,
        partial(
            aggregated_output,
            aggregator=lambda x, y: x + y,
            return_annotation="int",
        ),
    ),
}


@pytest.mark.parametrize(
    "factory", _WRAPPER_FACTORIES.values(), ids=list(_WRAPPER_FACTORIES)
)
def test_beartype_does_not_enforce_user_signature_on_wrapper(
    factory: Callable[[], Callable[..., Any]],
) -> None:
    """Beartype must treat a dags wrapper as a permissive `*args, **kwargs` forwarder.

    The user signature types `a` as `int` and `b` as `float`. Calling the
    beartype-decorated wrapper with a `str` for both violates that user view --
    but the wrapper genuinely forwards `*args, **kwargs`, so beartype (reading the
    forwarder `__annotations__`) must not raise.
    """
    wrapper = factory()
    checked = beartype(conf=_CONF)(wrapper)
    checked("not an int", b="not a float")


def _root(x: int) -> int:
    return x * 2


def _derived(_root: int, y: float) -> float:
    return _root + y


# The four ways `concatenate_functions` assembles the returned function: a bare
# `with_signature` wrapper (`return_type="tuple"`), a `*_output` converter over
# one (`list`/`dict`), or `aggregated_output` over one. `aggregator_return_type`
# is passed so the aggregator case does not emit a `DagsWarning` under
# `set_annotations=True`; it is harmlessly ignored under `set_annotations=False`.
_DAG_VARIANTS: dict[str, dict[str, Any]] = {
    "tuple": {"return_type": "tuple"},
    "list": {"return_type": "list"},
    "dict": {"return_type": "dict"},
    "aggregator": {
        "aggregator": lambda a, b: a + b,
        "aggregator_return_type": "float",
    },
}


@pytest.mark.parametrize(
    "set_annotations", [False, True], ids=["no_annotations", "set_annotations"]
)
@pytest.mark.parametrize("dag_kwargs", _DAG_VARIANTS.values(), ids=list(_DAG_VARIANTS))
def test_beartype_does_not_enforce_user_signature_on_concatenated_function(
    dag_kwargs: dict[str, Any],
    set_annotations: bool,
) -> None:
    """The genuine `concatenate_functions` output must read as a forwarder too.

    This exercises the real assembly path -- a `with_signature` wrapper, possibly
    nested inside a `*_output` converter whose `_apply_return_annotation` calls
    `get_annotations` on it -- across every `return_type` and both
    `set_annotations` values. The DAG's root params are `x: int` and `y: float`;
    `x=1.5` is a `float`, valid arithmetic but a violation of the `int` user
    annotation. With the forwarder shape on `__annotations__`, beartype must not
    raise.
    """
    concatenated = concatenate_functions(
        functions=[_root, _derived],
        set_annotations=set_annotations,
        **dag_kwargs,
    )
    checked = beartype(conf=_CONF)(concatenated)
    checked(x=1.5, y=2.0)
