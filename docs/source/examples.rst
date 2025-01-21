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


Functions from different namespaces
-----------------------------------

In large projects, function names can become lengthy when they share the same namespace.
Using dags, we can concatenate functions from different namespaces.

Suppose we define the following function in the module `linear_functions.py`:

.. code-block:: python

    def f(x):
        return 0.5 * x

In another module, called `parabolic_functions.py`, we define two more functions. Note,
that there is a function `f` in this module as well.

.. code-block:: python

    def f(x):
        return x**2

    def h(f, linear_functions__f):
        return (f + linear_functions__f) ** 2

The function `h` takes two inputs:
- `f` from `parabolic_functions.py`, referenced directly as f within the current
namespace.
- `f` from `linear_functions.py`, referenced using its namespace with a double
underscore separator (`linear_functions__f`).

Using `concatenate_functions_tree`, we are able to combine the functions from both
modules.

First, we need to define the functions tree, which maps functions to their namespace.
The functions tree can be nested to an arbitrary depth.

.. code-block:: python
    from linear_functions import f as linear_functions__f
    from parabolic_functions import f as parabolic_functions__f
    from parabolic_functions import h as parabolic_functions__h

    # Define functions tree
    functions = {
        "linear_functions": {"f": linear_functions__f},
        "parabolic_functions": {
            "f": parabolic_functions__f,
            "h": parabolic_functions__h
        },
    }

Next, we define the input structure, which maps the parameters of the functions to their
namespace. The input structure can also be created via the
`create_input_structure_tree` function.

.. code-block:: python
    # Define input structure
    input_structure = {
        "linear_functions": {"x": None},
        "parabolic_functions": {"x": None},
    }


Finally, we combine the functions using `concatenate_functions_tree`.

.. code-block:: python
    # Get combined function
    combined = concatenate_functions_tree(
        functions,
        input_structure=input_structure,
        targets={"parabolic_functions": {"h": None}},
    )

    # Call combined function
    combined(inputs={
        "linear_functions": {"x": 2},
        "parabolic_functions": {"x": 1},
    })

.. code-block:: python

    {"h": 4.0}

Importantly, dags does not allow for branches with trailing underscores in the
definition of the functions tree.

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
