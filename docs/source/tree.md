# Tree Structures

The `dags.tree` module provides utilities for working with nested dictionary structures, converting between tree representations and flat qualified names.

## Overview

When working with hierarchical data or organizing functions into namespaces, you often need to convert between:

- **Nested dictionaries** (tree structure): `{"a": {"b": 1, "c": 2}}`
- **Qualified names** (flat strings): `{"a__b": 1, "a__c": 2}`
- **Tree paths** (flat tuples): `{("a", "b"): 1, ("a", "c"): 2}`

```python
import dags.tree as dt
```

## Qualified Names

Qualified names (qnames) use a delimiter (default: `"__"`) to represent hierarchy:

```python
# The delimiter used for qualified names
print(dt.QNAME_DELIMITER)  # "__"

# Convert tree path to qname
qname = dt.qname_from_tree_path(("household", "income"))
# "household__income"

# Convert qname to tree path
path = dt.tree_path_from_qname("household__income")
# ("household", "income")
```

## Flattening and Unflattening

### To/From Qualified Names

```python
# Nested structure
tree = {
    "household": {
        "income": 50000,
        "expenses": 30000,
    },
    "taxes": {
        "federal": 10000,
        "state": 2500,
    },
}

# Flatten to qualified names
flat = dt.flatten_to_qnames(tree)
# {
#     "household__income": 50000,
#     "household__expenses": 30000,
#     "taxes__federal": 10000,
#     "taxes__state": 2500,
# }

# Unflatten back to tree
restored = dt.unflatten_from_qnames(flat)
# Returns the original nested structure
```

### To/From Tree Paths

```python
# Flatten to tuple paths
flat_paths = dt.flatten_to_tree_paths(tree)
# {
#     ("household", "income"): 50000,
#     ("household", "expenses"): 30000,
#     ("taxes", "federal"): 10000,
#     ("taxes", "state"): 2500,
# }

# Unflatten from tuple paths
restored = dt.unflatten_from_tree_paths(flat_paths)
```

## Extracting Names and Paths

```python
tree = {
    "a": {
        "x": 1,
        "y": 2,
    },
    "b": 3,
}

# Get all qualified names
names = dt.qnames(tree)
# ["a__x", "a__y", "b"]

# Get all tree paths
paths = dt.tree_paths(tree)
# [("a", "x"), ("a", "y"), ("b",)]
```

## Tree DAG Functions

The tree module also provides DAG functions that work with nested structures:

### create_dag_tree

Create a DAG from a nested function dictionary:

```python
functions = {
    "inputs": {
        "income": lambda: 50000,
        "tax_rate": lambda: 0.3,
    },
    "calculations": {
        "tax": lambda inputs__income, inputs__tax_rate: inputs__income * inputs__tax_rate,
        "net": lambda inputs__income, calculations__tax: inputs__income - calculations__tax,
    },
}

dag = dt.create_dag_tree(
    functions=functions,
    targets={"calculations": {"net": None}},  # or [("calculations", "net")]
)
```

### concatenate_functions_tree

Combine functions while preserving tree structure in outputs:

```python
combined = dt.concatenate_functions_tree(
    functions=functions,
    targets={"calculations": ["tax", "net"]},
)

result = combined()
# Returns nested dict: {"calculations": {"tax": 15000, "net": 35000}}
```

### functions_without_tree_logic

Convert tree-aware functions to flat functions:

```python
# Functions that use qualified names internally
tree_functions = {
    "a": {
        "x": lambda: 1,
        "y": lambda a__x: a__x + 1,  # references a__x
    },
}

# Convert to flat dict with qnames
flat_functions = dt.functions_without_tree_logic(tree_functions)
# {"a__x": <func>, "a__y": <func>}
```

## Validation Functions

The tree module includes validation utilities:

```python
# Check for invalid paths
dt.fail_if_paths_are_invalid(paths)

# Check for trailing underscores (not allowed)
dt.fail_if_path_elements_have_trailing_undersores(paths)

# Check for repeated top-level elements
dt.fail_if_top_level_elements_repeated_in_paths(paths)
```

## Real-World Example

Here's how pylcm uses tree utilities for regime management:

```python
import dags.tree as dt

# Define functions for multiple regimes
regime_functions = {
    "working": {
        "income": lambda wage, hours: wage * hours,
        "utility": lambda income, leisure: income + leisure,
    },
    "retired": {
        "income": lambda pension: pension,
        "utility": lambda income, leisure: income + 2 * leisure,
    },
}

# Flatten for processing with dags
flat_functions = dt.flatten_to_qnames(regime_functions)

# Use with concatenate_functions
import dags

combined = dags.concatenate_functions(
    functions=flat_functions,
    targets=["working__utility", "retired__utility"],
)

# Restore structure for output
result = combined(wage=20, hours=40, pension=1000, leisure=10)
nested_result = dt.unflatten_from_qnames(result)
# {"working": {"utility": ...}, "retired": {"utility": ...}}
```

## API Reference

See the [API documentation](api.md) for complete function signatures and details.
