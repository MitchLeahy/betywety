"""
Arbitrage opportunity detector.

This module identifies arbitrage opportunities where betting on both sides
of a market across different sportsbooks guarantees a profit.
"""

from typing import List, Dict, Optional
from src.utils.odds_converter import (
    american_to_implied_probability,
    calculate_arbitrage_profit,
    calculate_optimal_bet_sizing,
)


def detect_arbitrage(
    kalshi_market: Dict, other_market: Dict, min_profit_threshold: float = 1.0
) -> Optional[Dict]:
    """
    Detect arbitrage opportunity between Kalshi and another sportsbook.
    
    Args:
        kalshi_market: Market data from Kalshi with 'outcomes' list
        other_market: Market data from another sportsbook with 'outcomes' list
        min_profit_threshold: Minimum profit percentage to consider
    
    Returns:
        Arbitrage opportunity dict or None if no arbitrage found
    """
    if not kalshi_market.get("outcomes") or not other_market.get("outcomes"):
        return None
    
    # For two-way markets, we need to match outcomes
    # This is simplified - in reality, you'd need to match "Yes/No" or team names
    if len(kalshi_market["outcomes"]) != 2 or len(other_market["outcomes"]) != 2:
        return None
    
    # Get outcomes (assuming they're in the same order or matched)
    kalshi_yes = kalshi_market["outcomes"][0]
    kalshi_no = kalshi_market["outcomes"][1]
    other_yes = other_market["outcomes"][0]
    other_no = other_market["outcomes"][1]
    
    # Convert odds to implied probabilities
    kalshi_yes_prob = american_to_implied_probability(kalshi_yes["odds"])
    kalshi_no_prob = american_to_implied_probability(kalshi_no["odds"])
    other_yes_prob = american_to_implied_probability(other_yes["odds"])
    other_no_prob = american_to_implied_probability(other_no["odds"])
    
    # Check arbitrage scenarios:
    # 1. Bet Yes on Kalshi, No on other book
    prob_sum_1 = kalshi_yes_prob + other_no_prob
    profit_1 = calculate_arbitrage_profit(kalshi_yes_prob, other_no_prob)
    
    # 2. Bet No on Kalshi, Yes on other book
    prob_sum_2 = kalshi_no_prob + other_yes_prob
    profit_2 = calculate_arbitrage_profit(kalshi_no_prob, other_yes_prob)
    
    # Choose the better arbitrage opportunity
    if profit_1 > profit_2 and profit_1 >= min_profit_threshold:
        bet_sizing = calculate_optimal_bet_sizing(1000, kalshi_yes_prob, other_no_prob)
        return {
            "type": "arbitrage",
            "profit_percentage": profit_1,
            "kalshi_side": {
                "outcome": kalshi_yes["name"],
                "odds": kalshi_yes["odds"],
                "implied_prob": kalshi_yes_prob,
            },
            "other_side": {
                "source": other_market.get("source", "unknown"),
                "outcome": other_no["name"],
                "odds": other_no["odds"],
                "implied_prob": other_no_prob,
            },
            "optimal_bet_sizing": bet_sizing,
            "detected_at": kalshi_market.get("timestamp"),
        }
    elif profit_2 >= min_profit_threshold:
        bet_sizing = calculate_optimal_bet_sizing(1000, kalshi_no_prob, other_yes_prob)
        return {
            "type": "arbitrage",
            "profit_percentage": profit_2,
            "kalshi_side": {
                "outcome": kalshi_no["name"],
                "odds": kalshi_no["odds"],
                "implied_prob": kalshi_no_prob,
            },
            "other_side": {
                "source": other_market.get("source", "unknown"),
                "outcome": other_yes["name"],
                "odds": other_yes["odds"],
                "implied_prob": other_yes_prob,
            },
            "optimal_bet_sizing": bet_sizing,
            "detected_at": kalshi_market.get("timestamp"),
        }
    
    return None


def find_all_arbitrages(
    kalshi_markets: List[Dict],
    other_markets: List[Dict],
    min_profit_threshold: float = 1.0,
) -> List[Dict]:
    """
    Find all arbitrage opportunities across markets.
    
    Args:
        kalshi_markets: List of markets from Kalshi
        other_markets: List of markets from other sportsbooks
        min_profit_threshold: Minimum profit percentage
    
    Returns:
        List of arbitrage opportunities
    """
    arbitrages = []
    
    for kalshi_market in kalshi_markets:
        for other_market in other_markets:
            # In real implementation, you'd match markets first
            # using market_matcher utilities
            arb = detect_arbitrage(
                kalshi_market, other_market, min_profit_threshold
            )
            if arb:
                arb["market_id"] = kalshi_market.get("id")
                arb["event_name"] = kalshi_market.get("event_name")
                arbitrages.append(arb)
    
    return arbitrages
