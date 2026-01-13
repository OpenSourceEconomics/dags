# Getting Started

This guide introduces the core concepts of dags and shows you how to get started.

## Core Concept

dags works by analyzing function signatures to build a dependency graph. If a function has a parameter named `x`, and another function is named `x`, then the first function depends on the second.

```python
def x():
    return 1

def y(x):  # depends on x because parameter is named "x"
    return x + 1
```

## Basic Usage

### Creating a Combined Function

The main entry point is {func}`~dags.concatenate_functions`:

```python
import dags

def income():
    return 50000

def tax_rate():
    return 0.3

def tax(income, tax_rate):
    return income * tax_rate

def net_income(income, tax):
    return income - tax

# Combine all functions
combined = dags.concatenate_functions(
    functions={"income": income, "tax_rate": tax_rate, "tax": tax, "net_income": net_income},
    targets=["net_income", "tax"],
)

result = combined()
# result = {"net_income": 35000, "tax": 15000}
```

### Providing External Inputs

Functions can have parameters that are not provided by other functions. These become inputs to the combined function:

```python
def tax(income, tax_rate):
    return income * tax_rate

def net_income(income, tax):
    return income - tax

combined = dags.concatenate_functions(
    functions={"tax": tax, "net_income": net_income},
    targets=["net_income"],
)

# income and tax_rate are external inputs
result = combined(income=50000, tax_rate=0.3)
# result = {"net_income": 35000}
```

### Return Types

By default, `concatenate_functions` returns a dictionary. You can also get a tuple:

```python
combined = dags.concatenate_functions(
    functions=functions,
    targets=["a", "b", "c"],
    return_type="tuple",  # or "dict" (default)
)

a, b, c = combined()
```

## Inspecting the DAG

Use {func}`~dags.create_dag` to create and inspect the dependency graph:

```python
import dags

dag = dags.create_dag(
    functions={"income": income, "tax": tax, "net_income": net_income},
    targets=["net_income"],
)

# dag is a networkx.DiGraph
print(list(dag.nodes()))  # ['income', 'tax', 'net_income']
print(list(dag.edges()))  # [('income', 'tax'), ('tax', 'net_income'), ...]
```

## Finding Dependencies

Use {func}`~dags.get_ancestors` to find all functions that a target depends on:

```python
ancestors = dags.get_ancestors(
    functions=functions,
    targets=["net_income"],
)
# Returns set of all function names that net_income depends on
```

## Working with Annotations

### Getting Function Arguments

{func}`~dags.get_free_arguments` returns the parameter names of a function:

```python
def my_func(a, b, c=1):
    return a + b + c

args = dags.get_free_arguments(my_func)
# args = ["a", "b", "c"]
```

### Getting Type Annotations

{func}`~dags.get_annotations` returns type annotations as strings:

```python
def my_func(a: int, b: float) -> float:
    return a + b

annotations = dags.get_annotations(my_func)
# annotations = {"a": "int", "b": "float", "return": "float"}
```

## Renaming Arguments

Use {func}`~dags.rename_arguments` to change parameter names:

```python
def original(x, y):
    return x + y

renamed = dags.rename_arguments(original, mapper={"x": "a", "y": "b"})
# renamed now has signature (a, b) instead of (x, y)
```

This is useful when integrating functions from different sources that use different naming conventions.

## Next Steps

- Learn about [common usage patterns](usage_patterns.md) from real projects
- Explore [tree structures](tree.md) for nested data
- See the complete [API reference](api.md)
