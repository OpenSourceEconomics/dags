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


def monthly_earnings(x, earnings_limit):
    return x if x < earnings_limit else earnings_limit


def benefit(eligible, monthly_earnings, fraction):
    return monthly_earnings * fraction if eligible else 0
