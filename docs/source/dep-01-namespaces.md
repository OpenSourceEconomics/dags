(dep_namespaces)=

# DEP-01:

```{eval-rst}
+------------+------------------------------------------------------------------+
| Author     | `Hans-Martin von Gaudecker <https://github.com/hmgaudecker>`_    |
|            | `Janos Gabler <https://github.com/janosg>`_                      |
+------------+------------------------------------------------------------------+
| Status     | Draft                                                            |
+------------+------------------------------------------------------------------+
| Type       | Standards Track                                                  |
+------------+------------------------------------------------------------------+
| Created    | 2023-01-13                                                       |
+------------+------------------------------------------------------------------+
| Resolution |                                                                  |
+------------+------------------------------------------------------------------+
```

## Abstract

## Backwards compatibility

All changes are fully backwards compatible.

## Motivation

dags constructs a graph based on function names, which need to be valid Python
identifiers. In larger projects, which distribute things across many submodules, this
will lead to very long and/or cryptic identifiers. Even in smaller projects, this may
happen if there are multiple similar submodules. E.g., in case of a library depicting a
taxes and transfers system, two different transfer programs will have many similar
concepts (eligibility criteria, benefits, ...). In case one wants to process a panel
dataset with dimensions year × variables, often one will have to do this at a granular
level for some variables in some years, but at for all years at the same time for other
variables. In both cases, disambiguation via string manipulations on the user side is
repetitive and error-prone. As a result, code becomes unmanageable.

Hence, programming languages such as Python have the concept of namespaces, which allows
for short identifiers with the scope provided by the broader context. The equivalent for
dags will be a nested dictionary of functions based on
[pybaum](https://github.com/OpenSourceEconomics/pybaum), with the scope provided by a
branch of the tree.

Before going into detail, an example and some terminology are useful.


## Example: Taxes and transfers system

Say we have the following structure:

```
taxes_transfers
|-- __init__.py
|   |-- monthly_earnings(hours, hourly_wage)
|   |-- [call of concatenate_functions]
|-- pensions.py
|   |-- eligible(benefits_last_period, applied_this_period)
|   |-- benefit(eligible, aime, conversion_factor, monthly_earnings, unemployment_insurance__earnings_limit)
|-- unemployment_insurance.py
|   |-- monthly_earnings(hours, hourly_wage, earnings_limit):
|   |-- eligible(benefits_last_period, applied_this_period, hours, monthly_earnings, hours_limit, earnings_limit)
|   |-- benefit(eligible, monthly_earnings, fraction)
```

### Content of pensions.py
```py
def eligible(
    benefits_last_period,
    applied_this_period,
):
    return True if benefits_last_period else applied_this_period


def benefit(
    eligible,
    aime,
    conversion_factor,
    monthly_earnings,
    unemployment_insurance__earnings_limit,
):
    cond = eligible and monthly_earnings < unemployment_insurance__earnings_limit
    return aime * conversion_factor if cond else 0

import pensions
import unemployment_insurance

- repeat the example system from the test file
```

## Terminology

- **flat**
- **nested**
- **short**
- **registry**

## The new `functions` argument

### Possible Containers

- Registry: We only allow arbitrarily nested dictionaries with functions as leafs
- Only exception *might* be to enable providing a module directly instead of a
  dictionary of functions. The module name would be the outermost node of the branch,
  the functions in it would become leafs. However, it is unclear whether we'd want all
  functions imported in that module to show up in the graph or only the ones **def**ined
  in it.

  In any case, this would only be implemented in a second step, probably easier to put a
  helper recipe on the website.
- Hence, no unlabeled datastructures are possible.

It is probably not much extra work to do everything fully flexible but the flattening
rules will be very different from pybaum. Instead of flattening everything into lists we
need to flatten into dictionaries! Could re-use pybaum for that though. Just with
different registries.

### Argument names

- When can we use short and long names for arguments
- Are there ever ambiguous cases and if so, how are they handled? How does this relate
  to Python's resolution of scopes?
- What should not work (e.g. because it would be error prone)

**Use more abstract examples than tax and transfer here!**


```
abstract
|-- __init__.py
|   |-- a(b, c)
|   |-- [call of concatenate_functions]
|-- x.py
|   |-- f(a, b)
|   |-- g(f, c)
|-- y.py
|   |-- a(b, c)
|   |-- m(x__f, a, b)
```



## Inputs of the concatenated function

- Is it necessary to have different input modes (e.g. flat and nested) or is nested always what we want?
- input of `x__c` should inherit behavior of .


## Output of the concatenated function

- Is it necessary to have different output formats (e.g. flat and nested) or should this be done outside of dags?

## Implementation Ideas

- How aware should the dag be of the nesting? It is probably possible to flatten everything before a dag is built but then we lose information that could be useful for visualization.
- It is generally easy to flatten the function container. The hard part is converting the function arguments into long names that are globally unique in the dag!
- **What can we learn from pytask? Essentially pytask has this already!**
