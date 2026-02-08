"""
Utility functions for converting between different odds formats.
"""


def american_to_decimal(american_odds: float) -> float:
    """
    Convert American odds to decimal odds.
    
    Args:
        american_odds: American odds (e.g., -110, +150)
    
    Returns:
        Decimal odds (e.g., 1.91, 2.50)
    """
    if american_odds > 0:
        return (american_odds / 100) + 1
    else:
        return (100 / abs(american_odds)) + 1


def decimal_to_american(decimal_odds: float) -> float:
    """
    Convert decimal odds to American odds.
    
    Args:
        decimal_odds: Decimal odds (e.g., 1.91, 2.50)
    
    Returns:
        American odds (e.g., -110, +150)
    """
    if decimal_odds >= 2.0:
        return (decimal_odds - 1) * 100
    else:
        return -100 / (decimal_odds - 1)


def american_to_implied_probability(american_odds: float) -> float:
    """
    Convert American odds to implied probability.
    
    Args:
        american_odds: American odds (e.g., -110, +150)
    
    Returns:
        Implied probability as a decimal (e.g., 0.524 for 52.4%)
    """
    if american_odds > 0:
        return 100 / (american_odds + 100)
    else:
        return abs(american_odds) / (abs(american_odds) + 100)


def decimal_to_implied_probability(decimal_odds: float) -> float:
    """
    Convert decimal odds to implied probability.
    
    Args:
        decimal_odds: Decimal odds (e.g., 1.91, 2.50)
    
    Returns:
        Implied probability as a decimal (e.g., 0.524 for 52.4%)
    """
    return 1 / decimal_odds


def calculate_expected_value(
    probability: float, payout: float, stake: float = 100.0
) -> float:
    """
    Calculate expected value of a bet.
    
    Args:
        probability: True probability of winning (0-1)
        payout: Payout multiplier (e.g., 2.0 for 2x return)
        stake: Bet amount (default 100 for percentage calculation)
    
    Returns:
        Expected value as a percentage
    """
    ev = (probability * payout * stake) - ((1 - probability) * stake)
    return (ev / stake) * 100


def calculate_arbitrage_profit(prob_a: float, prob_b: float) -> float:
    """
    Calculate arbitrage profit percentage.
    
    Args:
        prob_a: Implied probability of side A
        prob_b: Implied probability of side B
    
    Returns:
        Profit percentage if arbitrage exists, else 0
    """
    total_prob = prob_a + prob_b
    if total_prob < 1.0:
        return (1.0 - total_prob) * 100
    return 0.0


def calculate_optimal_bet_sizing(
    total_investment: float, prob_a: float, prob_b: float
) -> dict:
    """
    Calculate optimal bet sizing for arbitrage opportunity.
    
    Args:
        total_investment: Total amount to invest
        prob_a: Implied probability of side A
        prob_b: Implied probability of side B
    
    Returns:
        Dictionary with bet amounts and guaranteed profit
    """
    total_prob = prob_a + prob_b
    
    if total_prob >= 1.0:
        return {
            "bet_a": 0.0,
            "bet_b": 0.0,
            "guaranteed_profit": 0.0,
            "profit_percentage": 0.0,
        }
    
    bet_a = total_investment * (prob_a / total_prob)
    bet_b = total_investment * (prob_b / total_prob)
    guaranteed_profit = total_investment * (1.0 - total_prob)
    profit_percentage = ((1.0 - total_prob) / total_prob) * 100
    
    return {
        "bet_a": round(bet_a, 2),
        "bet_b": round(bet_b, 2),
        "guaranteed_profit": round(guaranteed_profit, 2),
        "profit_percentage": round(profit_percentage, 2),
    }
