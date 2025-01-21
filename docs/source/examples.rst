Examples
========


A simple example
----------------

To understand what dags does, let's look at a few functions
that do simple calculations.

.. code-block:: python

    def f(x, y):
        return x**2 + y**2


    def g(y, z):
        return 0.5 * y * z


    def h(f, g):
        return g / f


Combine with a single target
----------------------------


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

Importantly, the order in which the functions are passed into ``concatenate_functions``
does not matter!


Combine with multiple targets
-----------------------------

Assume that we want the same combined h function as before but we also need intermediate
outputs. And we would like to have them as a dictionary. We can do this as follows:

.. code-block:: python

    combined = concatenate_functions(
        [h, f, g],
        targets=["h", "f", "g"],
        return_type="dict",
    )

    combined(x=1, y=2, z=3)

.. code-block:: python

    {"h": 0.6, "f": 5, "g": 3.0}


Renaming the output of a function
---------------------------------

So far, the name of the output of the function was determined from the ``__name__``
attribute of each function. This is not enough if you want to use dags to create
functions with exchangeable parts. Let's assume we have two implementations of f
and want to create combined functions for both versions.


.. code-block:: python

    import numpy as np


    def f_standard(x, y):
        return x**2 + y**2


    def f_numpy(x, y):
        return np.square(x) + np.square(y)

We can do that as follows:

.. code-block:: python

    combined_standard = concatenate_functions(
        {"f": f_standard, "g": g, "h": h},
        targets="h",
    )

    combined_numpy = concatenate_functions(
        {"f": f_numpy, "g": g, "h": h},
        targets="h",
    )

In fact, this ability to switch out components was the primary reason we wrote dags.
This functionality has, for example, been used in
`GETTSIM <https://github.com/iza-institute-of-labor-economics/gettsim>`_, a
framework to simulate reforms to the German tax and transfer system.


Renaming the input of functions
-------------------------------

Sometimes, we want to re-use a general function inside dags, but the arguments of that
function don't have the correct names. For example, we might have a general
implementation that we could re-use for `f`:


.. code-block:: python

    def sum_of_squares(a, b):
        return a**2 + b**2

Instead of writing a wrapper like:


.. code-block:: python

    def f(x, y):
        return sum_of_squares(a=x, b=y)

We can simply rename the arguments programmatically:

.. code-block:: python

    from dags.signature import rename_arguments

    functions = {
        "f": rename_arguments(sum_of_squares, mapper={"a": "x", "b": "y"}),
        "g": g,
        "h": h,
    }

    combined = concatenate_functions(functions, targets="h")
    combined(x=1, y=2, z=3)

.. code-block:: python

    0.6
