# dags

```{image} _static/images/logo.svg
:width: 400
:align: center
```

**dags** is a Python library for creating executable directed acyclic graphs (DAGs) from interdependent functions. It automatically determines the execution order based on function signatures and enables efficient composition of complex computational pipelines.

## Key Features

- **Automatic dependency resolution**: Functions are ordered based on their parameter names matching other functions' names
- **Function composition**: Combine multiple functions into a single callable
- **Tree structures**: Work with nested dictionaries using qualified names
- **Signature manipulation**: Rename arguments and manage function signatures

## Quick Example

```python
import dags

def a(x):
    return x ** 2

def b(a):
    return a + 1

def c(a, b):
    return a + b

# Combine functions into one
combined = dags.concatenate_functions(
    functions={"a": a, "b": b, "c": c},
    targets=["c"],
)

result = combined(x=5)  # Returns {"c": 51}
```

The key is that you can build the combined function at runtime, which allows you to
compose a computational pipeline in a way that you do not need to specify in advance, or
in a multitude of ways. It has proven very helpful in a framework to solve life cycle
models ([pylcm](https://github.com/OpenSourceEconomics/pylcm)) and to model the German
taxes and transfers system ([ttsim](https://github.com/ttsim-dev/ttsim) /
[gettsim](https://github.com/ttsim-dev/gettsim)).

## Installation

```bash
pip install dags
```

Or with conda:

```bash
conda install -c conda-forge dags
```

## Table of Contents

```{toctree}
:maxdepth: 2

getting_started
usage_patterns
tree
api
```
