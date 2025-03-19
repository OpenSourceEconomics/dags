# Changes

This is a record of all past dags releases and what went into them in reverse
chronological order. We follow [semantic versioning](https://semver.org/) and all
releases are available on [conda-forge](https://anaconda.org/conda-forge/dags).

## 0.3.0

- :gh:`29` Fix behavior of absolute/relative namespace handling (:ghuser:`hmgaudecker`)
- :gh:`30` Refactor `dag_tree` (:ghuser:`hmgaudecker`)
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
