# API Reference

## Core API

### `dags.concatenate_functions`

```python
concatenate_functions(
    functions: Mapping[str, Callable] | Sequence[Callable],
    targets: str | Sequence[str] | None = None,
    *,
    dag: nx.DiGraph | None = None,
    return_type: Literal["tuple", "list", "dict"] = "tuple",
    aggregator: Callable[[T, T], T] | None = None,
    aggregator_return_type: str | None = None,
    enforce_signature: bool = True,
    set_annotations: bool = False,
    lexsort_key: Callable[[str], Any] | None = None,
) -> Callable[..., Any]
```

Combine multiple interdependent functions into a single callable. Functions are executed
in topological order based on name-matching dependencies.

`dags.dag`

______________________________________________________________________

### `dags.create_dag`

```python
create_dag(
    functions: Mapping[str, Callable] | Sequence[Callable],
    targets: str | Sequence[str] | None,
) -> nx.DiGraph
```

Build a directed acyclic graph from functions without creating a combined function.

`dags.dag`

______________________________________________________________________

### `dags.get_ancestors`

```python
get_ancestors(
    functions: Mapping[str, Callable] | Sequence[Callable],
    targets: str | Sequence[str] | None,
    *,
    include_targets: bool = False,
) -> set[str]
```

Find all functions that a set of targets depends on.

`dags.dag`

______________________________________________________________________

## Annotations & Signatures

### `dags.get_annotations`

```python
get_annotations(
    func: Callable,
    *,
    eval_str: bool = False,
    default: str | type | None = None,
) -> dict[str, str] | dict[str, type]
```

Get type annotations from a function, handling `functools.partial` and Python 3.14 edge
cases. Returns a dict keyed by argument names plus `"return"`.

`dags.annotations`

______________________________________________________________________

### `dags.get_free_arguments`

```python
get_free_arguments(func: Callable) -> list[str]
```

Get parameter names of a function, excluding arguments bound via `functools.partial`.

`dags.annotations`

______________________________________________________________________

### `dags.rename_arguments`

```python
rename_arguments(
    func: Callable | None = None,
    *,
    mapper: Mapping[str, str],
) -> Callable
```

Rename function parameters using a mapping dict. Can be used as a decorator or called
directly.

`dags.signature`

______________________________________________________________________

### `dags.with_signature`

```python
with_signature(
    func: Callable | None = None,
    *,
    args: Mapping[str, str] | Sequence[str] | None = None,
    kwargs: Mapping[str, str] | Sequence[str] | None = None,
    enforce: bool = True,
    return_annotation: Any = inspect.Parameter.empty,
) -> Callable
```

Add a signature to a function of type `f(*args, **kwargs)`. Can be used as a decorator
with or without arguments. The dict form of `args`/`kwargs` maps parameter names to
type-hint strings and `return_annotation` sets the return type — both written to the
wrapper's `__signature__`. Default values cannot be set.

`dags.signature`

______________________________________________________________________

## Tree Utilities

All tree symbols are available from `dags.tree`.

### `dags.tree.QNAME_DELIMITER`

The delimiter used for qualified names: `"__"`.

`dags.tree.tree_utils`

______________________________________________________________________

### `dags.tree.qname_from_tree_path`

```python
qname_from_tree_path(tree_path: tuple[str, ...]) -> str
```

Convert a tree path tuple to a qualified name string (e.g., `("a", "b")` → `"a__b"`).

`dags.tree.tree_utils`

______________________________________________________________________

### `dags.tree.tree_path_from_qname`

```python
tree_path_from_qname(qname: str) -> tuple[str, ...]
```

Convert a qualified name string to a tree path tuple.

`dags.tree.tree_utils`

______________________________________________________________________

### `dags.tree.flatten_to_qnames`

```python
flatten_to_qnames(nested: NestedStructureDict) -> FlatQNameDict
```

Flatten a nested dict to a flat dict with qualified name keys.

`dags.tree.tree_utils`

______________________________________________________________________

### `dags.tree.unflatten_from_qnames`

```python
unflatten_from_qnames(flat_qnames: FlatQNameDict) -> NestedStructureDict
```

Restore a nested dict from a flat dict with qualified name keys.

`dags.tree.tree_utils`

______________________________________________________________________

### `dags.tree.flatten_to_tree_paths`

```python
flatten_to_tree_paths(nested: NestedStructureDict) -> FlatTreePathDict
```

Flatten a nested dict to a flat dict with tuple keys.

`dags.tree.tree_utils`

______________________________________________________________________

### `dags.tree.unflatten_from_tree_paths`

```python
unflatten_from_tree_paths(flat_tree_paths: FlatTreePathDict) -> NestedStructureDict
```

Restore a nested dict from a flat dict with tuple keys.

`dags.tree.tree_utils`

______________________________________________________________________

### `dags.tree.qnames`

```python
qnames(nested: NestedStructureDict) -> list[str]
```

Return a list of all qualified name keys from a nested dict.

`dags.tree.tree_utils`

______________________________________________________________________

### `dags.tree.tree_paths`

```python
tree_paths(nested: NestedStructureDict) -> list[tuple[str, ...]]
```

Return a list of all tree path keys from a nested dict.

`dags.tree.tree_utils`

______________________________________________________________________

## Tree DAG Functions

### `dags.tree.concatenate_functions_tree`

```python
concatenate_functions_tree(
    functions: NestedFunctionDict,
    inputs: NestedInputDict,
    targets: NestedTargetDict | None,
    *,
    enforce_signature: bool = True,
) -> Callable[[NestedInputDict], NestedOutputDict]
```

Combine a nested dictionary of functions into a single callable that takes nested inputs
and returns nested outputs.

`dags.tree.dag_tree`

______________________________________________________________________

### `dags.tree.create_dag_tree`

```python
create_dag_tree(
    functions: NestedFunctionDict,
    inputs: NestedInputDict,
    targets: NestedTargetDict | None,
) -> nx.DiGraph
```

Build a DAG from nested function dictionaries and input structure.

`dags.tree.dag_tree`

______________________________________________________________________

### `dags.tree.create_tree_with_input_types`

```python
create_tree_with_input_types(
    functions: NestedFunctionDict,
    targets: NestedTargetDict | None = None,
    top_level_inputs: set[str] | Sequence[str] = (),
) -> NestedInputStructureDict
```

Create a nested input structure template with type annotations based on functions and
targets. Useful for discovering the expected input shape before calling
`concatenate_functions_tree`.

`dags.tree.dag_tree`

______________________________________________________________________

### `dags.tree.get_functions_without_tree_logic`

```python
get_functions_without_tree_logic(
    functions: NestedFunctionDict,
    top_level_namespace: set[str],
) -> QNameFunctionDict
```

Flatten a nested function dict and rename all parameters to qualified absolute names,
producing a flat dict suitable for `concatenate_functions`.

`dags.tree.dag_tree`

______________________________________________________________________

### `dags.tree.get_one_function_without_tree_logic`

```python
get_one_function_without_tree_logic(
    function: Callable,
    tree_path: tuple[str, ...],
    top_level_namespace: set[str],
) -> Callable
```

Convert a single function from relative parameter names to qualified absolute names
given its tree path and top-level namespace.

`dags.tree.dag_tree`

______________________________________________________________________

## Tree Validation

All validation symbols are available from `dags.tree.validation`.

### `dags.tree.validation.fail_if_paths_are_invalid`

```python
fail_if_paths_are_invalid(
    functions: NestedFunctionDict | None = None,
    abs_qnames_functions: QNameFunctionDict | None = None,
    data_tree: NestedStructureDict | None = None,
    input_structure: NestedInputDict | None = None,
    targets: NestedTargetDict | None = None,
    top_level_namespace: set[str] | Sequence[str] = (),
) -> None
```

Validate all tree paths for trailing underscores and repeated top-level elements. Raises
`TrailingUnderscoreError` or `RepeatedTopLevelElementError`.

`dags.tree.validation`

______________________________________________________________________

### `dags.tree.validation.fail_if_path_elements_have_trailing_underscores`

```python
fail_if_path_elements_have_trailing_underscores(
    all_tree_paths: set[tuple[str, ...]],
) -> None
```

Check that no non-leaf path element ends with an underscore.

`dags.tree.validation`

______________________________________________________________________

### `dags.tree.validation.fail_if_top_level_elements_repeated_in_paths`

```python
fail_if_top_level_elements_repeated_in_paths(
    all_tree_paths: set[tuple[str, ...]],
    top_level_namespace: set[str],
) -> None
```

Fail if any top-level namespace element appears deeper in the hierarchy.

`dags.tree.validation`

______________________________________________________________________

### `dags.tree.validation.fail_if_top_level_elements_repeated_in_single_path`

```python
fail_if_top_level_elements_repeated_in_single_path(
    tree_path: tuple[str, ...],
    top_level_namespace: set[str],
) -> None
```

Same check as above, for a single path.

`dags.tree.validation`

______________________________________________________________________

## Exceptions

| Exception                       | Module                 | Description                                                                      |
| ------------------------------- | ---------------------- | -------------------------------------------------------------------------------- |
| `DagsError`                     | `dags.exceptions`      | Base exception for all dags errors                                               |
| `CyclicDependencyError`         | `dags.exceptions`      | DAG contains a cycle                                                             |
| `MissingFunctionsError`         | `dags.exceptions`      | Target functions not provided                                                    |
| `AnnotationMismatchError`       | `dags.exceptions`      | Type annotations conflict between functions                                      |
| `NonStringAnnotationError`      | `dags.exceptions`      | Reserved; not currently raised — non-string annotations are converted to strings |
| `InvalidFunctionArgumentsError` | `dags.exceptions`      | Invalid function arguments (too many positional, duplicated, unexpected keyword) |
| `ValidationError`               | `dags.exceptions`      | Base for validation errors                                                       |
| `RepeatedTopLevelElementError`  | `dags.tree.exceptions` | Top-level element repeated in tree path                                          |
| `TrailingUnderscoreError`       | `dags.tree.exceptions` | Non-leaf path element ends with underscore                                       |
