import inspect
from functools import partial

import pytest
from dags.dag import concatenate_functions
from dags.dag import create_dag
from dags.dag import get_ancestors


def pensions__eligible(pensions__benefits_last_period, pensions__applied_this_period):
    if pensions__benefits_last_period:
        out = True
    else:
        out = pensions__applied_this_period
    return out


def pensions__benefit(pensions__eligible, pensions__aime, pensions__conversion_factor):
    if pensions__eligible:
        out = pensions__aime * pensions__conversion_factor
    else:
        out = 0
    return out


def unemployment_insurance__eligible(
    unemployment_insurance__benefits_last_period,
    unemployment_insurance__applied_this_period,
    unemployment_insurance__hours_limit,
    unemployment_insurance__earnings_limit,
    hours,
    monthly_earnings,
):
    if (
        unemployment_insurance__benefits_last_period
        or hours < unemployment_insurance__hours_limit
        or monthly_earnings < unemployment_insurance__earnings_limit
    ):
        out = False
    else:
        out = unemployment_insurance__applied_this_period
    return out


def unemployment_insurance__benefit(
    unemployment_insurance__eligible,
    unemployment_insurance__last_wage,
    unemployment_insurance__fraction,
):
    if unemployment_insurance__eligible:
        out = unemployment_insurance__last_wage * unemployment_insurance__fraction
    else:
        out = 0
    return out


def monthly_earnings(hours, hourly_wage):
    return hours * 4.3 * hourly_wage


functions = {
    "pensions": {
        "eligible": pensions__eligible,
        "benefit": pensions__benefit,
    },
    "unemployment_insurance": {
        "eligible": unemployment_insurance__eligible,
        "benefit": unemployment_insurance__benefit,
    },
    "employment": {
        "monthly_earnings": monthly_earnings,
    },
}


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
    targets = {}
