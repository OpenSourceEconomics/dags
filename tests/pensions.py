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
