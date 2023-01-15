(dep_namespaces)=

# DEP-01:

```{eval-rst}
+------------+------------------------------------------------------------------+
| Author     | `Hans-Martin von Gaudecker <https://github.com/hmgaudecker>`_    |
|            | `Janos Gabler <https://github.com/janosg>`_                      |
+------------+------------------------------------------------------------------+
| Status     | Accepted                                                         |
+------------+------------------------------------------------------------------+
| Type       | Standards Track                                                  |
+------------+------------------------------------------------------------------+
| Created    | 2022-01-28                                                       |
+------------+------------------------------------------------------------------+
| Resolution |                                                                  |
+------------+------------------------------------------------------------------+
```

## Abstract




## Backwards compatibility

All changes are fully backwards compatible.

## Motivation

- gettsim motivation
- panel dags motivation

## Example: Tax and transfer system

- repeat the example system from the test file

## The new `functions` argument

### Possible Containers

- should we only allow (arbitrarily?) nested dictionaries or everything that works in pybaum?
- do we need an extensible registry as in pybaum?
- should it be possible to provide a module directly instead of a dictionary of functions?
- do we allow unlabeled datastructures as long as the functions have a `__name__` attribute?

It is probably not much extra work to do everything fully flexible but the flattening rules will be very different from pybaum. Instead of flattening everything into lists we need to flatten into dictionaries! Could re-use pybaum for that though. Just with different registries.


### Argument names

- When can we use short and long names for arguments
- Are there ever ambiguous cases and if so, how are they handled? How does this relate to pythons resolution of scopes?
- What should not work (e.g. because it would be error prone)

**Use more abstract examples than tax and transfer here!**


## Inputs of the concatenated function

- Is it necessary to have different input modes (e.g. flat and nested) or is nested always what we want?


## Output of the concatenated function

- Is it necessary to have different output formats (e.g. flat and nested) or should this be done outside of dags?


## Implementation Ideas

- How aware should the dag be of the nesting? It is probably possible to flatten everything before a dag is built but then we lose information that could be useful for visualization.
- It is generally easy to flatten the function container. The hard part is converting the function arguments into long names that are globally unique in the dag!
- **What can we learn from pytask? Essentially pytask has this already!**
