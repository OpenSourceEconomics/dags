# API Reference

This page documents the public API of the dags library.

## Core Functions

The main functions for creating and executing DAGs.

### concatenate_functions

```{eval-rst}
.. autofunction:: dags.concatenate_functions
```

### create_dag

```{eval-rst}
.. autofunction:: dags.create_dag
```

### get_ancestors

```{eval-rst}
.. autofunction:: dags.get_ancestors
```

## Annotation Functions

Functions for working with type annotations and function signatures.

### get_annotations

```{eval-rst}
.. autofunction:: dags.get_annotations
```

### get_free_arguments

```{eval-rst}
.. autofunction:: dags.get_free_arguments
```

### rename_arguments

```{eval-rst}
.. autofunction:: dags.rename_arguments
```

## Exceptions

```{eval-rst}
.. autoexception:: dags.DagsError
   :show-inheritance:

.. autoexception:: dags.AnnotationMismatchError
   :show-inheritance:

.. autoexception:: dags.CyclicDependencyError
   :show-inheritance:

.. autoexception:: dags.InvalidFunctionArgumentsError
   :show-inheritance:

.. autoexception:: dags.MissingFunctionsError
   :show-inheritance:

.. autoexception:: dags.ValidationError
   :show-inheritance:
```

## dags.tree

The tree module provides utilities for working with nested dictionaries and qualified names.

### Tree Path Utilities

```{eval-rst}
.. autodata:: dags.tree.QNAME_DELIMITER

.. autofunction:: dags.tree.qname_from_tree_path

.. autofunction:: dags.tree.tree_path_from_qname

.. autofunction:: dags.tree.qnames

.. autofunction:: dags.tree.tree_paths
```

### Flatten/Unflatten Functions

```{eval-rst}
.. autofunction:: dags.tree.flatten_to_qnames

.. autofunction:: dags.tree.unflatten_from_qnames

.. autofunction:: dags.tree.flatten_to_tree_paths

.. autofunction:: dags.tree.unflatten_from_tree_paths
```

### Tree DAG Functions

```{eval-rst}
.. autofunction:: dags.tree.create_dag_tree

.. autofunction:: dags.tree.concatenate_functions_tree

.. autofunction:: dags.tree.create_tree_with_input_types

.. autofunction:: dags.tree.functions_without_tree_logic

.. autofunction:: dags.tree.one_function_without_tree_logic
```

### Validation Functions

```{eval-rst}
.. autofunction:: dags.tree.fail_if_paths_are_invalid

.. autofunction:: dags.tree.fail_if_path_elements_have_trailing_undersores

.. autofunction:: dags.tree.fail_if_top_level_elements_repeated_in_paths

.. autofunction:: dags.tree.fail_if_top_level_elements_repeated_in_single_path
```

### Tree Exceptions

```{eval-rst}
.. autoexception:: dags.tree.RepeatedTopLevelElementError
   :show-inheritance:

.. autoexception:: dags.tree.TrailingUnderscoreError
   :show-inheritance:
```
