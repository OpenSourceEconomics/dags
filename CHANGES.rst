Changes
=======

This is a record of all past dags releases and what went into them in reverse
chronological order. We follow `semantic versioning <https://semver.org/>`_ and all
releases are available on `Anaconda.org
<https://anaconda.org/OpenSourceEconomics/dags>`_.


0.2.2 - 2022-xx-xx
------------------

- :gh:`5` Updates examples used in tests (:ghuser:`janosg`)
- :gh:`7` improves the examples in the test cases.
- :gh:`10` turns ``targets`` into an optional argument. All variables in the DAG are
  returned by default.
- :gh:`9` Add function to return the DAG. Check for cycles in DAG.
  (:ghuser:`ChristianZimpelmann`)

0.2.1 - 2022-03-29
------------------

- :gh:`4` Small fix in treatment of partialled arguments (:ghuser:`janosg`)


0.2.0 - 2022-03-25
------------------

- :gh:`3` ignores partialled arguments when reading signatures (:ghuser:`janosg`)
- :gh:`2` enforces signatures of generated functions, adds support for more output
  types and adds decorators to work with signatures (:ghuser:`janosg`)


0.1.0 - 2022-03-08
------------------

- :gh:`1` releases the initial version of dags.
