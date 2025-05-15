# models.py
from sqlalchemy import (
    Table, Column, Integer, String, Date, Float, Enum, MetaData,
    UniqueConstraint, ForeignKey, JSON
)
try:
    from sqlalchemy.dialects.postgresql import JSONB
except ImportError:
    # Use regular JSON for SQLite compatibility
    JSONB = JSON

metadata = MetaData()

symbols = Table(
    "symbols", metadata,
    Column("id", Integer, primary_key=True),
    Column("ticker", String, unique=True, nullable=False),
    Column("name", String),
)

# --- HISTORICAL & TTM ----------------------------------------------------
period_type_enum = Enum("annual", "quarter", name="period_type")

financial_periods = Table(
    "financial_periods", metadata,
    Column("id", Integer, primary_key=True),
    Column("symbol_id", Integer, ForeignKey("symbols.id"), nullable=False),
    Column("period_type", period_type_enum, nullable=False),
    Column("period_end", Date, nullable=False),
    UniqueConstraint("symbol_id", "period_type", "period_end")
)

financial_metrics = Table(
    "financial_metrics", metadata,
    Column("id", Integer, primary_key=True),
    Column("period_id", Integer, ForeignKey("financial_periods.id"), nullable=False),
    Column("metric_code", String, nullable=False),
    Column("value", Float),
)

# --- ANALYST ESTIMATES ---------------------------------------------------
analyst_estimates = Table(
    "analyst_estimates", metadata,
    Column("id", Integer, primary_key=True),
    Column("symbol_id", Integer, ForeignKey("symbols.id"), nullable=False),
    Column("fiscal_year", Integer, nullable=False),
    Column("payload", JSON),           # { revenue: x, ebitda: y, ... }
    UniqueConstraint("symbol_id", "fiscal_year")
)

# --- PEER TABLES (operating + trading) -----------------------------------
peer_operating = Table(
    "peer_operating", metadata,
    Column("id", Integer, primary_key=True),
    Column("host_symbol_id", Integer, ForeignKey("symbols.id"), nullable=False),
    Column("peer_ticker", String, nullable=False),
    Column("payload", JSON),
    UniqueConstraint("host_symbol_id", "peer_ticker")
)

peer_trading = Table(
    "peer_trading", metadata,
    Column("id", Integer, primary_key=True),
    Column("host_symbol_id", Integer, ForeignKey("symbols.id"), nullable=False),
    Column("peer_ticker", String, nullable=False),
    Column("payload", JSON),
    UniqueConstraint("host_symbol_id", "peer_ticker")
)

# --- BROKER PRICE TARGETS ------------------------------------------------
price_targets = Table(
    "price_targets", metadata,
    Column("id", Integer, primary_key=True),
    Column("symbol_id", Integer, ForeignKey("symbols.id"), nullable=False),
    Column("broker", String),
    Column("analyst", String),
    Column("research_date", Date),
    Column("target_px", Float),
    Column("direction", Enum("up", "down", "same", name="pt_dir")),
)
