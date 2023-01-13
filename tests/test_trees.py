import inspect
from functools import partial

import pensions
import unemployment_insurance
from dags.dag import concatenate_functions
from dags.dag import create_dag
from dags.dag import get_ancestors


# Define a function in the "global" namespace
def monthly_earnings(hours, hourly_wage):
    return hours * 4.3 * hourly_wage


# Note there is the same function in unemployment_insurance.py,
# leading to a potential ambiguity of what is meant with "monthly_earnings"
# inside of the "unemployment_insurance" namespace (namespace in the dags sense).
# In particular, there would be no way to access the "global" function.
#
# To handle this case in different manners, add a keyword argument:
#
#    concatenate_functions(
#        ...
#        name_clashes="**raise**|ignore|warn",
#    )
#
# Default is to raise an error.


# Typical way would be to define functions in a nested dictionary creating namespaces.
functions = {
    "pensions": {
        "eligible": pensions.eligible,
        "benefit": pensions.benefit,
    },
    "unemployment_insurance": {
        "eligible": unemployment_insurance.eligible,
        "benefit": unemployment_insurance.benefit,
    },
    "monthly_earnings": monthly_earnings,
}


# Typical way to define targets would be in a nested dictionary.
targets = {"pensions": ["benefit"], "unemployment_insurance": ["benefit"]}

# Alternatively, use flat structure.
targets_flat = {"pensions__benefit", "unemployment_insurance__benefit"}

# Expected behavior of concatenate_functions with input_mode="tree"
# (meaning that a nested dict of inputs is expected by the complete system)
# and output_mode="tree" is to return *targets*
#
# concatenate_functions(
#   functions=functions,
#   targets=targets,
#   input_mode="tree",
#   output_mode="tree"
# )
#
def complete_system(
    pensions: dict,  # str: float: aime, conversion_factor, benefits_last_period, applied_this_period
    unemployment_insurance: dict,  # benefits_last_period, applied_this_period, hours_limit, earnings_limit,
    hours: float,
    hourly_wage: float,
):
    ...


# Expected behavior of concatenate_functions with input_mode="flat"
# (meaning that long name-style input is expected by the complete system)
# and output_mode="tree" is to return *targets_flat*
#
# concatenate_functions(
#   functions=functions,
#   targets=targets_flat,
#   input_mode="flat",
#   output_mode="flat"
# )


def complete_system(
    pensions__aime,
    pensions__conversion_factor,
    pensions__benefits_last_period,
    pensions__applied_this_period,
    unemployment_insurance__benefits_last_period,
    unemployment_insurance__applied_this_period,
    unemployment_insurance__hours_limit,
    unemployment_insurance__earnings_limit,
    employment__hours,
    employment__hourly_wage,
):
    ...


functions = {
    "pensions": {
        "eligible": pensions.eligible,
        "benefit": pensions.benefit,
    },
    "unemployment_insurance": {
        "eligible": unemployment_insurance.eligible,
        "benefit": unemployment_insurance.benefit,
    },
    "monthly_earnings": monthly_earnings,
}


# Specifying functions in a flat manner.
#
# (difference to current implementation is that we can still use
# the short names inside of modules)
#
# OTOneH, not sure whether we want to support this, added for completeness.
# OTOtherH, it's just a matter of calling tree_unflatten (I think)

functions_flat = {
    "pensions__eligible": pensions.eligible,
    "pensions__benefit": pensions.benefit,
    "unemployment_insurance__eligible": unemployment_insurance.eligible,
    "unemployment_insurance__benefit": unemployment_insurance.benefit,
    "monthly_earnings": monthly_earnings,
}


# concatenate_functions(
#   functions=functions_flat,
#   targets=targets_flat,
#   functions_mode="flat",
#   input_mode="flat",
#   output_mode="flat"
# )


def complete_system(
    pensions__aime,
    pensions__conversion_factor,
    pensions__benefits_last_period,
    pensions__applied_this_period,
    unemployment_insurance__benefits_last_period,
    unemployment_insurance__applied_this_period,
    unemployment_insurance__hours_limit,
    unemployment_insurance__earnings_limit,
    hours,
    hourly_wage,
):
    ...
