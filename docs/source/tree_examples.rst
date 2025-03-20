
Examples with functions trees
=============================

There are two modes of using ``dags.tree``:

1. Loose mode, where dags tries to infer the required inputs from the topology of the
   functions tree alone. This means that you cannot use inputs at the top-level
   namespace and that you cannot use relative paths. *(The reason is that we need to
   take all paths containing double underscores as absolute paths.)*
2. Fixed top-level namespace mode, where names of all elements in the top-level
   namespace are fixed before attempting to infer required inputs. This means that
   inputs in the top-level namespace and relative paths are allowed. However, no element
   of the top level namespace may be repeated at deeper levels of nesting. *(The reason
   is that to distinguish between absolute and relative paths, we check whether the
   first element of a path is a key in the top-level namespace.)*



Functions from different namespaces
-----------------------------------

In large projects, function names can become lengthy when they share the same namespace.
Using dags, we can concatenate functions from different namespaces.

Suppose we define the following function in the module `linear_functions.py`:

.. code-block:: python

    def g(x):
        return 0.5 * x

    def g_deriv(f, x):
        return 0.5


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
        "parabolic_functions": {"x": 2},
    })

.. code-block:: python

    {"h": 3.0}

Importantly, dags does not allow for branches with trailing underscores in the
definition of the functions tree.


The input structure and two different behaviors
-----------------------------------------------



You might think that in the above example, you may have the ability to pass a common
value of `x` in the top-level namespace. Using the above default behavior this is not
the case. The problem is that we do not have a way to tell absolute paths in the tree
(i.e., those starting at the top-level namespace) from relative paths (i.e., those
starting at the current namespace) when we create the input structure or when we
concatenate the functions.

Consider the following example:

.. code-block:: python

    def a(x):
        return x**2

    functions = {
        "nested": {"a" a}
    }


When creating an input structure, both of the following could be valid:

.. code-block:: python

    input_structure_a = {
        "x": None
    }

    input_structure_b = {
        "nested": {"x": None}
    }

Default behavior
________________

The default behavior of ``dags.tree`` is to disallow any functions or inputs in the
top-level namespace. That is, ``concatenate_functions_tree`` will raise an error if you
pass ``input_structure_a`` into the above example. ``create_input_structure_tree`` will
return ``input_structure_b``.

Technically speaking, the default behavior is to distinguish between leaf names (no
double underscores present in an argument, clearly local to a function) and qualified
names (has at least one double underscore in an argument). This implies two things:

1. Leaf names are not allowed in the top-level namespace, i.e., all functions and inputs
   must be nested at least one level deep.
2. All qualified names are interpreted as absolute paths.


Alternative: Fixing the top-level namespace
___________________________________________

In complex projects, the default behavior can be limiting the usefulness of the
hierarchical structure of the functions tree because absolute paths need to be provided
all the time. Hence, we provide an alternative behavior that allows for the top-level
namespace to be fixed.

In the above example, we can either fix the top-level namespace to be ``{"a", "x"}`` or
``{"a"}``. In the first case, we need to provide ``input_structure_a``, in the second
case, we need to provide ``input_structure_b``.

What is disallowed (and checked by ``dags.tree``) is to have path elements that are
identical to an element of the top-level namespace.
