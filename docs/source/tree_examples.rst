
Examples with functions trees
=============================

It is often useful to structure code in a hierarchical way, e.g. to group related
functions together. In ``dags``, this can be achieved by defining a so-called
"functions tree".


Functions from different namespaces
-----------------------------------

In large projects, function names can become lengthy when they share the same namespace.
Using dags, we can concatenate functions from different namespaces.

Suppose we define the following function in the module `linear.py`:

.. code-block:: python

    # linear.py

    def f(x):
        return 0.5 * x

In another module, called `parabolic.py`, we define two more functions. Note,
that there is a function `f` in this module as well.

.. code-block:: python

    # parabolic.py

    def f(x):
        return x**2

    def h(f, linear__f):
        return (f + linear__f) ** 2

The function `h` takes two inputs:

- `f` from `parabolic.py`, referenced directly as f within the current namespace.
- `f` from `linear.py`, referenced using its namespace with a double underscore
  separator (`linear__f`).

Using `concatenate_functions_tree`, we are able to combine the functions from both
modules.

First, we need to define the functions tree, which maps functions to their namespace.
The functions tree can be nested to an arbitrary depth.

.. code-block:: python

    import linear
    import parabolic

    # Define functions tree
    functions = {
        "linear": {"f": linear.f},
        "parabolic": {
            "f": parabolic.f,
            "h": parabolic.h
        },
    }

Next, we create the input structure, which maps the parameters of the functions to their
namespace. The input structure can also be created via the
`create_tree_with_input_types` function.

.. code-block:: python

    import dags.tree as dt

    input_structure = dt.create_tree_with_input_types(functions=functions)
    input_structure

.. code-block:: python

    {
        'linear': {'x': None},
        'parabolic': {'x': None},
    }

Finally, we combine the functions using `concatenate_functions_tree`.

.. code-block:: python

    # Get combined function
    combined = concatenate_functions_tree(
        functions=functions,
        input_structure=input_structure,
        targets={"parabolic": {"h": None}},
    )

    # Call combined function
    combined(
        inputs={
            "linear": {"x": 1},
            "parabolic": {"x": 2},
        }
    )

Top-level inputs
________________

Note that `create_tree_with_input_types` created two inputs with leaf names ``x``. You
might have thought that only one ``x`` should be provided at the top level. This is the
distinction between absolute and relative paths.

We can just provide the top-level input ``x``:

.. code-block:: python

    combined_top_level = dt.concatenate_functions_tree(
        functions,
        input_structure={"x": None},
        targets={"parabolic": {"h": None}},
    )
    combined_top_level(inputs={"x": 3})

.. code-block:: python

    {'parabolic': {'h': 110.25}}

By default, ``create_tree_with_input_types`` assumes that all required input paths are
relative to the location where they are defined. If you need to provide paths at the top
level, you can do so by passing the ``top_level_inputs`` argument to
``create_tree_with_input_types``:

.. code-block:: python

    input_structure = dt.create_tree_with_input_types(
        functions=functions,
        top_level_inputs={"x": None},
    )
    input_structure

.. code-block:: python

    {'x': None}


Caveats
-------

Importantly, dags does not allow trailing underscores in elements of the function tree's
paths. Since we are using double underscores to separate elements, this would yield a
triple underscore and the round trip would not be unique if it were allowed.

There must not be any elements in the function tree's paths at one or more levels of
nesting that are identical to an element of the top-level namespace. The reason is that
in order to decide whether a path, say ``("a", "b")``, is absolute or relative, we
check whether the first element of the path is a key in the top-level namespace.

A note on terminology
---------------------

The basic structure of a pytree we work with is a nested dictionary, say

.. code-block:: python

    {
        "a": {"b": f, "c": 2},
        "d": {"e": {"f": 3}, "g": g},
    }

We refer to the elements of the top-level namespace as ``a`` and ``d``.

The set of tree paths is ``{("a", "b"), ("a", "c"), ("d", "e", "f"), ("d", "g")}``. We can represent the
pytree as a "flat tree paths" dictionary with tree paths as keys:

.. code-block:: python

    {
        ("a", "b"): f,
        ("a", "c"): 2,
        ("d", "e", "f"): 3,
        ("d", "g"): g,
    }

Tree paths thus are always tuples referring to absolute paths in the pytree.

Similarly, the set of qualified names in the strict sense is ``{"a__b", "a__c",
"d__e__f", "d__g"}``. We can represent the pytree as a "flat qualified names" dictionary
with qualified names as keys:

.. code-block:: python

    {
        "a__b": f,
        "a__c": 2,
        "d__e__f": 3,
        "d__g": g,
    }

However, we can also have relative paths in function arguments provided by the user. For
example, the function ``g`` may take the argument ``e__f``, which would resolve to the
tree path ``("d", "e", "f")``, i.e. the qualified name in the strict sense ``d__e__f``.
Sometimes, however, we need to refer to the relative path ``("e__f")`` as a qualified
name.
