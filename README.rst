dags
====

.. start-badges

.. image:: https://img.shields.io/pypi/v/dags?color=blue
    :alt: PyPI
    :target: https://pypi.org/project/dags

.. image:: https://img.shields.io/pypi/pyversions/dags
    :alt: PyPI - Python Version
    :target: https://pypi.org/project/dags

.. image:: https://img.shields.io/conda/vn/conda-forge/dags.svg
    :target: https://anaconda.org/conda-forge/dags

.. image:: https://img.shields.io/conda/pn/conda-forge/dags.svg
    :target: https://anaconda.org/conda-forge/dags

.. image:: https://img.shields.io/pypi/l/dags
    :alt: PyPI - License
    :target: https://pypi.org/project/dags

.. image:: https://readthedocs.org/projects/dags/badge/?version=latest
    :target: https://dags.readthedocs.io/en/latest

.. image:: https://img.shields.io/github/workflow/status/OpenSourceEconomics/dags/main/main
   :target: https://github.com/OpenSourceEconomics/dags/actions?query=branch%3Amain

.. image:: https://codecov.io/gh/OpenSourceEconomics/dags/branch/main/graph/badge.svg
    :target: https://codecov.io/gh/OpenSourceEconomics/dags

.. image:: https://results.pre-commit.ci/badge/github/OpenSourceEconomics/dags/main.svg
    :target: https://results.pre-commit.ci/latest/github/OpenSourceEconomics/dags/main
    :alt: pre-commit.ci status

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
    :target: https://github.com/psf/black

.. end-badges

About
-----

dags provides tools to combine several interrelated functions into one function.
The order in which the functions are called is determined by a topological sort on
a dag that is constructed from the function signatures. You can specify which of the
function results will be returned in the combined function.

dags is a tiny library, all the hard work is done by the great
`NetworkX <https://networkx.org/documentation/stable/tutorial.html>`_

Example
-------

To understand what dags does, let's look at a very simple example of a few functions
that do simple calculations.

.. code-block:: python

    def f(x, y):
        return x**2 + y**2


    def g(y, z):
        return 0.5 * y * z


    def h(f, g):
        return g / f


Assume that we are interested in a function that calculates h, given x, y and z.

We could hardcode this function as:

.. code-block:: python

    def hardcoded_combined(x, y, z):
        _f = f(x, y)
        _g = g(y, z)
        return h(_f, _g)


    hardcoded_combined(x=1, y=2, z=3)

.. code-block:: python

    0.6

Instead, we can use dags to construct the same function:

.. code-block:: python

    from dags import concatenate_functions

    combined = concatenate_functions([h, f, g], targets="h")

    combined(x=1, y=2, z=3)

.. code-block:: python

    0.6

More examples can be found in the `documentation <https://dags.readthedocs.io/en/latest/#>`_


Notable features
----------------

- The dag is constructed while the combined function is created and does not cause too
  much overhead when the function is called.
- If all individual functions are jax compatible, the combined function is jax compatible.
- When jitted or vmapped with jax, we havenot seen any performance loss compared to
  hard coding the combined function.
- Whene there is more than one target, you can determine whether the result is returned
  as tuple, list or dict or pass in an aggregator to combine the multiple outputs.
- Since the relationships are discoverd from function signatures, dags provides
  decorators to rename arguments.


Installation
------------

dags is available on `PyPI <https://pypi.org/project/dags>`_ and `Anaconda.org
<https://anaconda.org/conda-forge/dags>`_. Install it with

.. code-block:: console

    $ pip install dags

    # or

    $ conda install -c conda-forge dags

Documentation
-------------

The `documentation <https://dags.readthedocs.io/en/latest/#>`_ is hosted on Read the Docs.
