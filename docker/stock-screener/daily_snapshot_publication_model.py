"""ORM models for authoritative Daily Snapshot response publications."""

from sqlalchemy import Column, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.sql import func

from ..database import Base


class DailySnapshotPublicationInput(Base):
    """Committed non-analytics leaves required to rebuild one publication."""

    __tablename__ = "daily_snapshot_publication_inputs"

    id = Column(Integer, primary_key=True)
    market = Column(String(8), nullable=False)
    scan_id = Column(
        String(36),
        ForeignKey("scans.scan_id", ondelete="CASCADE"),
        nullable=False,
    )
    analytics_version = Column(String(64), nullable=False)
    snapshot_schema_version = Column(Integer, nullable=False)
    input_version = Column(String(64), nullable=False)
    key_markets_json = Column(Text, nullable=False)
    key_markets_sha256 = Column(String(64), nullable=False)
    captured_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        UniqueConstraint(
            "market",
            "scan_id",
            "analytics_version",
            "snapshot_schema_version",
            name="uq_daily_snapshot_publication_input_identity",
        ),
        Index(
            "idx_daily_snapshot_publication_input_lookup",
            "market",
            "scan_id",
            "analytics_version",
            "snapshot_schema_version",
        ),
    )


class DailySnapshotPublication(Base):
    __tablename__ = "daily_snapshot_publications"

    id = Column(Integer, primary_key=True)
    market = Column(String(8), nullable=False)
    scan_id = Column(
        String(36),
        ForeignKey("scans.scan_id", ondelete="CASCADE"),
        nullable=False,
    )
    analytics_version = Column(String(64), nullable=False)
    snapshot_schema_version = Column(Integer, nullable=False)
    publication_version = Column(String(64), nullable=False)
    payload_json = Column(Text, nullable=False)
    payload_sha256 = Column(String(64), nullable=False)
    generated_at = Column(DateTime(timezone=True), nullable=False)
    published_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        UniqueConstraint(
            "market",
            "scan_id",
            "analytics_version",
            "snapshot_schema_version",
            name="uq_daily_snapshot_publication_identity",
        ),
        Index(
            "idx_daily_snapshot_publication_lookup",
            "market",
            "scan_id",
            "analytics_version",
            "snapshot_schema_version",
        ),
    )
