def monthly_earnings(hours, hourly_wage, earnings_limit):
    e = hours * 4.3 * hourly_wage
    return e if e < earnings_limit else earnings_limit


def eligible(
    benefits_last_period,
    applied_this_period,
    hours,
    monthly_earnings,
    hours_limit,
    earnings_limit,
):
    cond = (
        benefits_last_period or hours < hours_limit or monthly_earnings < earnings_limit
    )

    return False if cond else applied_this_period


def benefit(eligible, baseline_earnings, fraction):
    return baseline_earnings * fraction if eligible else 0
