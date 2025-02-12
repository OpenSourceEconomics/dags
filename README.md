# dags

<!-- start-badges -->

[![PyPI](https://img.shields.io/pypi/v/dags?color=blue)](https://pypi.org/project/dags)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/dags)](https://pypi.org/project/dags)
[![Conda Version](https://img.shields.io/conda/vn/conda-forge/dags.svg)](https://anaconda.org/conda-forge/dags)
[![Conda Platform](https://img.shields.io/conda/pn/conda-forge/dags.svg)](https://anaconda.org/conda-forge/dags)
[![PyPI - License](https://img.shields.io/pypi/l/dags)](https://pypi.org/project/dags)
[![Documentation Status](https://readthedocs.org/projects/dags/badge/?version=latest)](https://dags.readthedocs.io/en/latest)
[![GitHub Workflow Status](https://img.shields.io/github/workflow/status/OpenSourceEconomics/dags/main/main)](https://github.com/OpenSourceEconomics/dags/actions?query=branch%3Amain)
[![codecov](https://codecov.io/gh/OpenSourceEconomics/dags/graph/badge.svg?token=jKu3vvz98M)](https://codecov.io/gh/OpenSourceEconomics/dags)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/OpenSourceEconomics/dags/main.svg)](https://results.pre-commit.ci/latest/github/OpenSourceEconomics/dags/main)

<!-- end-badges -->

## About

dags provides tools to combine several interrelated functions into one function. The
order in which the functions are called is determined by a topological sort on a dag
that is constructed from the function signatures. You can specify which of the function
results will be returned in the combined function.

dags is a tiny library, all the hard work is done by the great
[NetworkX](https://networkx.org/documentation/stable/tutorial.html).

## Example

To understand what dags does, let's look at a very simple example of a few functions
that do simple calculations.

```python
def f(x, y):
    return x**2 + y**2


def g(y, z):
    return 0.5 * y * z


def h(f, g):
    return g / f
```

Assume that we are interested in a function that calculates h, given x, y and z.

We could hardcode this function as:

```python
def hardcoded_combined(x, y, z):
    _f = f(x, y)
    _g = g(y, z)
    return h(_f, _g)


hardcoded_combined(x=1, y=2, z=3)
```

```python
0.6
```

Instead, we can use dags to construct the same function:

```python
from dags import concatenate_functions

combined = concatenate_functions([h, f, g], targets="h")

combined(x=1, y=2, z=3)
```

```python
0.6
```

More examples can be found in the
[documentation](https://dags.readthedocs.io/en/latest/#)

## Notable features

- The dag is constructed while the combined function is created and does not cause too
  much overhead when the function is called.
- If all individual functions are jax compatible, the combined function is jax
  compatible.
- When jitted or vmapped with jax, we have not seen any performance loss compared to
  hard coding the combined function.
- When there is more than one target, you can determine whether the result is returned
  as tuple, list or dict or pass in an aggregator to combine the multiple outputs.
- Since the relationships are discoverd from function signatures, dags provides
  decorators to rename arguments in order to make it easy to wrap functions you do not
  control yourself.

## Installation

dags is available on [PyPI](https://pypi.org/project/dags) and
[conda-forge](https://anaconda.org/conda-forge/dags). Install it with

```console
$ pip install dags

# or

$ pixi add dags

# or

$ conda install -c conda-forge dags
```

## Documentation

The [documentation](https://dags.readthedocs.io/en/latest/#) is hosted on Read the Docs.
