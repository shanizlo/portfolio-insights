"""
Database setup and all table definitions in one place.
We use SQLAlchemy for the ORM — it creates the tables automatically on startup.
"""
import os
from datetime import datetime
from sqlalchemy import (
    create_engine, Column, String, Integer, Numeric,
    Date, DateTime, ForeignKey, UniqueConstraint, JSON, text
)
from sqlalchemy.orm import sessionmaker, DeclarativeBase

# Read DB URL from environment, fall back to local default
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@db:5432/portfolio"
)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)


# FastAPI dependency — yields a DB session for each request, closes it after
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ── Base class all models inherit from ──────────────────────────────────────
class Base(DeclarativeBase):
    pass


# ── Reference tables (fully replaced on each upload) ────────────────────────

class Customer(Base):
    __tablename__ = "customers"
    customer_id  = Column(String(32), primary_key=True)
    name         = Column(String(256))
    join_date    = Column(Date)
    tenure_years = Column(Numeric(6, 2))
    segment      = Column(String(32))


class Stock(Base):
    __tablename__ = "stocks"
    ticker       = Column(String(16), primary_key=True)
    company_name = Column(String(256))
    exchange     = Column(String(16))
    currency     = Column(String(3))
    sector       = Column(String(64))
    country      = Column(String(32))


class AccountMap(Base):
    __tablename__ = "account_map"
    id               = Column(Integer, primary_key=True, autoincrement=True)
    customer_id      = Column(String(32))
    external_account = Column(String(64))
    segment_tag      = Column(String(32))


class DiscountRule(Base):
    __tablename__ = "discount_rules"
    id                      = Column(Integer, primary_key=True, autoincrement=True)
    min_portfolio_value_usd = Column(Numeric(18, 2))
    min_tenure_years        = Column(Numeric(6, 2))
    base_discount_pct       = Column(Numeric(6, 4))
    tenure_bonus_pct        = Column(Numeric(6, 4))


# ── Fact tables (append-only, duplicates skipped via unique constraints) ─────

class Trade(Base):
    __tablename__ = "trades"
    id             = Column(Integer, primary_key=True, autoincrement=True)
    customer_id    = Column(String(32), ForeignKey("customers.customer_id"))
    ticker         = Column(String(16), ForeignKey("stocks.ticker"))
    trade_date     = Column(Date, nullable=False)
    side           = Column(String(4), nullable=False)   # BUY or SELL
    quantity       = Column(Integer, nullable=False)
    price          = Column(Numeric(18, 6), nullable=False)
    trade_currency = Column(String(3), nullable=False)
    price_usd      = Column(Numeric(18, 6))              # price converted to USD
    fee_usd        = Column(Numeric(18, 6))              # null = fee unknown
    source         = Column(String(32))
    ingested_at    = Column(DateTime, default=datetime.utcnow)
    __table_args__ = (
        UniqueConstraint("customer_id", "ticker", "trade_date", "side", "quantity", "price", name="uq_trade"),
    )


class HoldingSnapshot(Base):
    __tablename__ = "holdings_snapshot"
    id          = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(String(32), ForeignKey("customers.customer_id"))
    ticker      = Column(String(16), ForeignKey("stocks.ticker"))
    quantity    = Column(Integer, nullable=False)
    as_of_date  = Column(Date, nullable=False)
    ingested_at = Column(DateTime, default=datetime.utcnow)
    __table_args__ = (
        UniqueConstraint("customer_id", "ticker", "as_of_date", name="uq_holding"),
    )


class PriceHistory(Base):
    __tablename__ = "price_history"
    id          = Column(Integer, primary_key=True, autoincrement=True)
    ticker      = Column(String(16), ForeignKey("stocks.ticker"))
    price_date  = Column(Date, nullable=False)
    close       = Column(Numeric(18, 6), nullable=False)
    currency    = Column(String(3), nullable=False)
    close_usd   = Column(Numeric(18, 6))
    ingested_at = Column(DateTime, default=datetime.utcnow)
    __table_args__ = (
        UniqueConstraint("ticker", "price_date", name="uq_price"),
    )


class FxRate(Base):
    __tablename__ = "fx_rates"
    id           = Column(Integer, primary_key=True, autoincrement=True)
    currency     = Column(String(3), nullable=False)
    rate_date    = Column(Date, nullable=False)
    usd_per_unit = Column(Numeric(18, 6), nullable=False)
    __table_args__ = (
        UniqueConstraint("currency", "rate_date", name="uq_fx_rate"),
    )


# ── Job tracking (one row per uploaded file) ─────────────────────────────────

class IngestionJob(Base):
    __tablename__ = "ingestion_jobs"
    id                = Column(String(64), primary_key=True)
    file_type         = Column(String(32), nullable=False)
    status            = Column(String(16), default="pending")  # pending | done | failed
    records_processed = Column(Integer, default=0)
    warning_count     = Column(Integer, default=0)
    error_count       = Column(Integer, default=0)
    error_detail      = Column(JSON)   # list of {row, level, message}
    created_at        = Column(DateTime, default=datetime.utcnow)
    completed_at      = Column(DateTime)


def init_db():
    Base.metadata.create_all(bind=engine)