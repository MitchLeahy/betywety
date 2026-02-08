"""Bronze layer models - raw data storage."""

from sqlalchemy import Column, String, DateTime, Integer, JSON
from sqlalchemy.dialects.postgresql import UUID, JSONB
import uuid
from datetime import datetime
from src.models.database import Base


class RawLine(Base):
    """Raw API response storage - Bronze layer."""
    
    __tablename__ = "raw_lines"
    __table_args__ = {"schema": "bronze"}
    
    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        index=True
    )
    source = Column(String(50), nullable=False, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    api_endpoint = Column(String(255))
    raw_response = Column(JSONB, nullable=False)
    response_status = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    def __repr__(self):
        return f"<RawLine(id={self.id}, source={self.source}, timestamp={self.timestamp})>"
