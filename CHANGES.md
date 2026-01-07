# Changes

This is a record of all past dags releases and what went into them in reverse
chronological order. We follow [semantic versioning](https://semver.org/) and all
releases are available on [conda-forge](https://anaconda.org/conda-forge/dags).

## 0.4.2

- :gh:`59` Add Python 3.14 support, fix annotation extraction for wrapped functions
  (:ghuser:`hmgaudecker`).
- :gh:`58` Move from mypy to ty (:ghuser:`hmgaudecker`).

## 0.4.1

- :gh:`52` Only import from typing_extensions if TYPE_CHECKING (:ghuser:`timmens`).

## 0.4.0

- :gh:`49` Optionally use lexicographical sort to control execution order
  (:ghuser:`hmgaudecker`).
- :gh:`48` Make type hints more expressive (:ghuser:`timmens`).
- :gh:`47` Rename qual_name -> qname (:ghuser:`hmgaudecker`).
- :gh:`45` Make creation of DAG optional in calling concatenate_functions
  (:ghuser:`hmgaudecker`).
- :gh:`44` Annotation behavior updates and bug fixes (:ghuser:`timmens`).
- :gh:`43` Refactor annotation handling (:ghuser:`timmens`).
- :gh:`42` Sort the outputs of the top-level namespace and format line-wise
  (:ghuser:`hmgaudecker`).
- :gh:`41` Implement `get_input_types` function (:ghuser:`timmens`).
- :gh:`40` Add new custom exceptions (:ghuser:`timmens`).
- :gh:`37` Validate consistency of type annotations during function-creation time
  (:ghuser:`timmens`).

## 0.3.0

- :gh:`33` Simplify interfaces, expose more functions (:ghuser:`hmgaudecker`)
- :gh:`29` Improve namespace handling, allow for relative paths (:ghuser:`hmgaudecker`)
- :gh:`31` Refactor `dag_tree` (:ghuser:`hmgaudecker`)
- :gh:`28` Expose relevant functions for working with function trees (:ghuser:`hmgaudecker`)
- :gh:`26` Allow Unicode characters in Python identifiers (:ghuser:`MImmesberger`, :ghuser:`hmgaudecker`).
- :gh:`24` Add type hints to the codebase (:ghuser:`timmens`, :ghuser:`hmgaudecker`).
- :gh:`23` Update infrastructure, move to Pixi (:ghuser:`timmens`).
- :gh:`17` Add possibility to use namespaces (:ghuser:`lars-reimann`, :ghuser:`MImmesberger`).

## 0.2.3

- :gh:`9` Add function to return the DAG. Check for cycles in DAG.
  (:ghuser:`ChristianZimpelmann`)

## 0.2.2

- :gh:`5` Updates examples used in tests (:ghuser:`janosg`)
- :gh:`7` improves the examples in the test cases.
- :gh:`10` turns ``targets`` into an optional argument. All variables in the DAG are
  returned by default. (:ghuser:`tobiasraabe`)

## 0.2.1

- :gh:`4` Small fix in treatment of partialled arguments (:ghuser:`janosg`)

## 0.2.0

- :gh:`3` ignores partialled arguments when reading signatures (:ghuser:`janosg`)
- :gh:`2` enforces signatures of generated functions, adds support for more output
  types and adds decorators to work with signatures (:ghuser:`janosg`)

## 0.1.0

- :gh:`1` releases the initial version of dags.
