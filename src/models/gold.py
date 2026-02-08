"""Gold layer models - business intelligence data."""

from sqlalchemy import Column, String, DateTime, Boolean, Numeric, ForeignKey
from sqlalchemy.dialects.postgresql import UUID, JSONB
import uuid
from datetime import datetime
from src.models.database import Base


class MatchedMarket(Base):
    """Matched markets across different sources - Gold layer."""
    
    __tablename__ = "matched_markets"
    __table_args__ = {"schema": "gold"}
    
    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        index=True
    )
    event_name = Column(String(255), nullable=False, index=True)
    sport = Column(String(50), index=True)
    market_type = Column(String(50), index=True)
    kalshi_line_id = Column(
        UUID(as_uuid=True),
        ForeignKey("silver.betting_lines.id"),
        nullable=False
    )
    other_line_id = Column(
        UUID(as_uuid=True),
        ForeignKey("silver.betting_lines.id"),
        nullable=False
    )
    other_source = Column(String(50), nullable=False)
    matched_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    confidence_score = Column(Numeric(3, 2))  # 0.00 to 1.00
    
    def __repr__(self):
        return (
            f"<MatchedMarket(id={self.id}, event={self.event_name}, "
            f"confidence={self.confidence_score})>"
        )


class Opportunity(Base):
    """Opportunistic betting opportunities - Gold layer."""
    
    __tablename__ = "opportunities"
    __table_args__ = {"schema": "gold"}
    
    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        index=True
    )
    matched_market_id = Column(
        UUID(as_uuid=True),
        ForeignKey("gold.matched_markets.id"),
        nullable=False,
        index=True
    )
    type = Column(String(50), default="opportunistic")  # 'opportunistic'
    kalshi_side = Column(JSONB, nullable=False)
    other_side = Column(JSONB, nullable=False)
    expected_value = Column(Numeric(5, 2))  # Percentage
    detected_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    expires_at = Column(DateTime, index=True)
    is_active = Column(Boolean, default=True, index=True)
    
    def __repr__(self):
        return (
            f"<Opportunity(id={self.id}, ev={self.expected_value}%, "
            f"active={self.is_active})>"
        )


class ArbitrageOpportunity(Base):
    """Arbitrage opportunities - Gold layer."""
    
    __tablename__ = "arbitrage_opportunities"
    __table_args__ = {"schema": "gold"}
    
    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        index=True
    )
    matched_market_id = Column(
        UUID(as_uuid=True),
        ForeignKey("gold.matched_markets.id"),
        nullable=False,
        index=True
    )
    kalshi_side = Column(JSONB, nullable=False)
    other_side = Column(JSONB, nullable=False)
    profit_percentage = Column(Numeric(5, 2), nullable=False)  # Percentage
    optimal_bet_sizing = Column(JSONB, nullable=False)
    detected_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    expires_at = Column(DateTime, index=True)
    is_active = Column(Boolean, default=True, index=True)
    
    def __repr__(self):
        return (
            f"<ArbitrageOpportunity(id={self.id}, profit={self.profit_percentage}%, "
            f"active={self.is_active})>"
        )
