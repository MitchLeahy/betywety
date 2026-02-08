"""Silver layer models - cleaned and normalized data."""

from sqlalchemy import Column, String, DateTime, Boolean, Numeric, ForeignKey, Text
from sqlalchemy.dialects.postgresql import UUID, JSONB
import uuid
from datetime import datetime
from src.models.database import Base


class BettingLine(Base):
    """Cleaned and normalized betting lines - Silver layer."""
    
    __tablename__ = "betting_lines"
    __table_args__ = {"schema": "silver"}
    
    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        index=True
    )
    timestamp = Column(DateTime, nullable=False, index=True)
    source = Column(String(50), nullable=False, index=True)
    event_name = Column(String(255), nullable=False, index=True)
    sport = Column(String(50), index=True)
    market_type = Column(String(50), index=True)
    market_description = Column(Text)
    outcome_name = Column(String(255), nullable=False)
    outcome_odds = Column(Numeric(10, 2))  # American odds format
    outcome_odds_decimal = Column(Numeric(10, 4))  # Decimal odds
    line_value = Column(Numeric(10, 2))  # For spreads/totals
    implied_probability = Column(Numeric(5, 4))  # 0.0000 to 1.0000
    is_active = Column(Boolean, default=True, index=True)
    event_start_time = Column(DateTime)
    bronze_id = Column(
        UUID(as_uuid=True),
        ForeignKey("bronze.raw_lines.id"),
        nullable=True
    )
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    def __repr__(self):
        return (
            f"<BettingLine(id={self.id}, source={self.source}, "
            f"event={self.event_name}, outcome={self.outcome_name}, "
            f"odds={self.outcome_odds})>"
        )
