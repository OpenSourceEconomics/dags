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


# Functions are defined in a nested dictionary creating namespaces.
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


# Targets are defined in a nested dictionary as well.
targets = {"pensions": ["benefit"], "unemployment_insurance": ["benefit"]}

input_structure = {
    "pensions": {
        "aime": None,
        "conversion_factor": None,
        "benefits_last_period": None,
        "applied_this_period": None,
    },
    "unemployment_insurance": {
        "benefits_last_period": None,
        "applied_this_period": None,
        "hours_limit": None,
        "earnings_limit": None,
    },
    "hours": None,
    "hourly_wage": None,
}

# Expected behavior of concatenate_functions_tree with mode="tree" (meaning that a
# nested dict of input_structure is expected by the complete system and short-style
# function names are possible to use)
concatenate_functions_tree(
    functions=functions,
    targets=targets,
    mode="tree",
    input_structure=input_structure,
)


def complete_system(
    pensions: dict,  # str: float: aime, conversion_factor, benefits_last_period, applied_this_period
    unemployment_insurance: dict,  # benefits_last_period, applied_this_period, hours_limit, earnings_limit,
    hours: float,
    hourly_wage: float,
):
    ...
