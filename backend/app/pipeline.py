"""
Ingestion pipeline — one function per file type.

All functions follow the same pattern:
  1. Parse the file (CSV or Excel) into a Pandas DataFrame
  2. Check required columns are present
  3. Validate rows (skip bad ones, log warnings/errors)
  4. Write valid rows to the DB (skip duplicates via unique constraints)
  5. Return a summary dict

Reference tables (customers, stocks, etc.) are fully replaced on each upload.
Fact tables (trades, holdings, prices, fx_rates) are append-only.
"""
import io
import logging
from datetime import datetime

import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.db import (
    Customer, Stock, AccountMap, DiscountRule,
    Trade, HoldingSnapshot, PriceHistory, FxRate
)

logger = logging.getLogger(__name__)

SUPPORTED_CURRENCIES = {"USD", "GBP", "EUR", "ILS"}


# ── Shared helpers ───────────────────────────────────────────────────────────

def parse_file(content: bytes, filename: str) -> pd.DataFrame:
    """Read CSV or Excel bytes into a DataFrame. Both formats work identically after this."""
    if filename.endswith((".xlsx", ".xls")):
        return pd.read_excel(io.BytesIO(content), sheet_name=0)
    return pd.read_csv(io.BytesIO(content))


def to_dates(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Parse date columns into Python date objects. Handles mixed formats gracefully."""
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], dayfirst=False, errors="coerce").dt.date
    return df


def get_fx(db: Session, currency: str, on_date) -> float:
    """
    Look up the USD exchange rate for a currency on a given date.
    Falls back to the most recent prior rate if no exact match.
    """
    if currency == "USD":
        return 1.0
    rate = (
        db.query(FxRate)
        .filter(FxRate.currency == currency, FxRate.rate_date <= on_date)
        .order_by(FxRate.rate_date.desc())
        .first()
    )
    return float(rate.usd_per_unit) if rate else 1.0


def make_result():
    """Return a fresh result summary dict."""
    return {"processed": 0, "warnings": 0, "errors": 0, "detail": []}


def warn(result, row_num, msg):
    result["warnings"] += 1
    result["detail"].append({"row": row_num, "level": "warning", "message": msg})


def error(result, row_num, msg):
    result["errors"] += 1
    result["detail"].append({"row": row_num, "level": "error", "message": msg})


def check_columns(df: pd.DataFrame, required: list[str]):
    """Raise immediately if any required column is missing."""
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


# ── File type handlers ───────────────────────────────────────────────────────

def ingest_stocks(df: pd.DataFrame, db: Session) -> dict:
    """
    Upsert behavior — insert or update one row per ticker.
    If a specific row fails to apply, record an error but continue
    processing the rest of the file.
    """
    check_columns(df, ["Ticker", "CompanyName", "Exchange", "Currency", "Sector", "Country"])
    result = make_result()

    seen_tickers: set[str] = set()

    for i, (_, row) in enumerate(df.iterrows(), start=2):
        ticker = row["Ticker"]

        # Skip duplicate tickers within the same file — first one wins
        if ticker in seen_tickers:
            error(result, i, f"Duplicate ticker in file: {ticker} (skipping)")
            continue
        seen_tickers.add(ticker)

        values = {
            "ticker":       ticker,
            "company_name": row["CompanyName"],
            "exchange":     row["Exchange"],
            "currency":     row["Currency"],
            "sector":       row["Sector"],
            "country":      row["Country"],
        }

        try:
            stmt = insert(Stock).values(**values)
            stmt = stmt.on_conflict_do_update(
                index_elements=[Stock.ticker],
                set_={
                    "company_name": stmt.excluded.company_name,
                    "exchange":     stmt.excluded.exchange,
                    "currency":     stmt.excluded.currency,
                    "sector":       stmt.excluded.sector,
                    "country":      stmt.excluded.country,
                },
            )
            db.execute(stmt)
            db.commit()
            result["processed"] += 1
        except SQLAlchemyError as exc:
            db.rollback()
            error(result, i, f"Failed to upsert stock {ticker}: {exc}")

    return result


def ingest_customers(df: pd.DataFrame, db: Session) -> dict:
    """Full replace — delete all customers and re-insert."""
    check_columns(df, ["CustomerID", "CustomerName", "JoinDate", "TenureYears", "Segment"])
    result = make_result()
    df = to_dates(df, ["JoinDate"])

    db.query(Customer).delete()
    for _, row in df.iterrows():
        db.add(Customer(
            customer_id  = row["CustomerID"],
            name         = row["CustomerName"],
            join_date    = row["JoinDate"],
            tenure_years = float(row["TenureYears"]),
            segment      = row["Segment"],
        ))

    db.commit()
    result["processed"] = len(df)
    return result


def ingest_account_map(df: pd.DataFrame, db: Session) -> dict:
    """Full replace. Orphaned accounts (no matching customer) are flagged but still stored."""
    check_columns(df, ["cust_id", "external_account", "SegmentTag"])
    result = make_result()

    valid_customers = {r[0] for r in db.query(Customer.customer_id).all()}
    db.query(AccountMap).delete()

    for i, (_, row) in enumerate(df.iterrows(), start=2):
        if row["cust_id"] not in valid_customers:
            warn(result, i, f"Orphaned account — no matching customer: {row['cust_id']}")
        db.add(AccountMap(
            customer_id      = row["cust_id"] if row["cust_id"] in valid_customers else None,
            external_account = row["external_account"],
            segment_tag      = row["SegmentTag"],
        ))

    db.commit()
    result["processed"] = len(df)
    return result


def ingest_discount_rules(df: pd.DataFrame, db: Session) -> dict:
    """Full replace."""
    check_columns(df, ["MinPortfolioValueFromUSD", "MinTenureYears", "BaseDiscountPct", "TenureBonusPct"])
    result = make_result()

    db.query(DiscountRule).delete()
    for _, row in df.iterrows():
        db.add(DiscountRule(
            min_portfolio_value_usd = float(row["MinPortfolioValueFromUSD"]),
            min_tenure_years        = float(row["MinTenureYears"]),
            base_discount_pct       = float(row["BaseDiscountPct"]),
            tenure_bonus_pct        = float(row["TenureBonusPct"]),
        ))

    db.commit()
    result["processed"] = len(df)
    return result


def ingest_fx_rates(df: pd.DataFrame, db: Session) -> dict:
    """Append-only. Duplicate (currency, date) pairs are skipped."""
    check_columns(df, ["Currency", "Date", "USD_per_unit"])
    result = make_result()
    df = to_dates(df, ["Date"])
    rows = []

    for i, (_, row) in enumerate(df.iterrows(), start=2):
        if not row["Date"]:
            error(result, i, "Invalid date")
            continue
        rows.append({"currency": row["Currency"], "rate_date": row["Date"], "usd_per_unit": float(row["USD_per_unit"])})

    if rows:
        db.execute(insert(FxRate).values(rows).on_conflict_do_nothing(constraint="uq_fx_rate"))
        db.commit()

    result["processed"] = len(rows)
    return result


def ingest_prices(df: pd.DataFrame, db: Session) -> dict:
    """Append-only. Duplicate (ticker, date) pairs are skipped."""
    check_columns(df, ["Ticker", "Date", "Close", "Currency"])
    result = make_result()
    df = to_dates(df, ["Date"])
    valid_tickers = {r[0] for r in db.query(Stock.ticker).all()}
    rows = []

    for i, (_, row) in enumerate(df.iterrows(), start=2):
        price_date = row["Date"]

        # Treat NaT / NaN / empty strings as invalid dates
        if pd.isna(price_date) or str(price_date).strip() in ("", "NaT", "nan", "NaN"):
            error(result, i, "Invalid date")
            continue
        if row["Ticker"] not in valid_tickers:
            warn(result, i, f"Unknown ticker: {row['Ticker']}")
        rows.append({
            "ticker":     row["Ticker"],
            "price_date": price_date,
            "close":      float(row["Close"]),
            "currency":   row["Currency"],
            "close_usd":  float(row["Close"]),  # USD conversion happens at query time via fx_rates
        })

    if rows:
        db.execute(insert(PriceHistory).values(rows).on_conflict_do_nothing(constraint="uq_price"))
        db.commit()

    result["processed"] = len(rows)
    return result


def ingest_holdings(df: pd.DataFrame, db: Session) -> dict:
    """Append-only. Duplicate (customer, ticker, date) triplets are skipped."""
    check_columns(df, ["CustomerID", "Ticker", "Quantity", "AsOfDate"])
    result = make_result()
    df = to_dates(df, ["AsOfDate"])
    valid_customers = {r[0] for r in db.query(Customer.customer_id).all()}
    valid_tickers   = {r[0] for r in db.query(Stock.ticker).all()}
    rows = []

    for i, (_, row) in enumerate(df.iterrows(), start=2):
        if not row["AsOfDate"]:
            error(result, i, "Invalid date")
            continue
        if row["CustomerID"] not in valid_customers:
            warn(result, i, f"Unknown customer: {row['CustomerID']} (skipping row)")
            continue
        if row["Ticker"] not in valid_tickers:
            warn(result, i, f"Unknown ticker: {row['Ticker']} (skipping row)")
            continue
        rows.append({
            "customer_id": row["CustomerID"],
            "ticker":      row["Ticker"],
            "quantity":    int(row["Quantity"]),
            "as_of_date":  row["AsOfDate"],
        })

    if rows:
        db.execute(insert(HoldingSnapshot).values(rows).on_conflict_do_nothing(constraint="uq_holding"))
        db.commit()

    result["processed"] = len(rows)
    return result


def ingest_trades(df: pd.DataFrame, db: Session, source: str = "unknown") -> dict:
    """
    Append-only. Each row is validated before insert:
    - BUY trades must have positive quantity
    - SELL trades must have negative quantity (we store absolute value)
    - Unknown customers/tickers get a warning but are still written
    - Fee is stored as null if missing (not assumed to be zero)
    """
    check_columns(df, ["Customer_ID", "tradeDate", "ticker", "Side", "Quantity", "Px", "TradeCurrency"])
    result = make_result()
    df = to_dates(df, ["tradeDate"])
    valid_customers = {r[0] for r in db.query(Customer.customer_id).all()}
    valid_tickers   = {r[0] for r in db.query(Stock.ticker).all()}
    rows = []

    for i, (_, row) in enumerate(df.iterrows(), start=2):
        if not row["tradeDate"]:
            error(result, i, "Invalid trade date")
            continue

        side = str(row["Side"]).upper()
        qty  = int(row["Quantity"])

        # A BUY with negative quantity (or SELL with positive) is a data error
        if side == "BUY" and qty < 0:
            error(result, i, f"BUY trade has negative quantity: {qty}")
            continue
        if side == "SELL" and qty > 0:
            error(result, i, f"SELL trade has positive quantity (expected negative): {qty}")
            continue

        if row["Customer_ID"] not in valid_customers:
            warn(result, i, f"Unknown customer: {row['Customer_ID']}")
        if row["ticker"] not in valid_tickers:
            warn(result, i, f"Unknown ticker: {row['ticker']}")
        if row["TradeCurrency"] not in SUPPORTED_CURRENCIES:
            warn(result, i, f"Unsupported currency: {row['TradeCurrency']}")

        price    = float(row["Px"])
        currency = row["TradeCurrency"]
        fx       = get_fx(db, currency, row["tradeDate"])

        # Fee: store null if missing — do NOT assume 0 (see open questions in design doc)
        fee_raw = row.get("FeeUSD", "")
        fee_usd = float(fee_raw) if str(fee_raw) not in ("", "nan", "None") else None

        rows.append({
            "customer_id":    row["Customer_ID"],
            "ticker":         row["ticker"],
            "trade_date":     row["tradeDate"],
            "side":           side,
            "quantity":       abs(qty),
            "price":          price,
            "trade_currency": currency,
            "price_usd":      price * fx,
            "fee_usd":        fee_usd,
            "source":         source,
        })

    if rows:
        db.execute(insert(Trade).values(rows).on_conflict_do_nothing(constraint="uq_trade"))
        db.commit()

    result["processed"] = len(rows)
    return result


# ── Router — maps file_type string to the right handler function ─────────────

HANDLERS = {
    "stocks":         ingest_stocks,
    "customers":      ingest_customers,
    "account-map":    ingest_account_map,
    "discount-rules": ingest_discount_rules,
    "fx-rates":       ingest_fx_rates,
    "prices":         ingest_prices,
    "holdings":       ingest_holdings,
    "trades":         ingest_trades,
}


def run_pipeline(file_type: str, content: bytes, filename: str, db: Session, source: str = None) -> dict:
    """
    Entry point called by the worker.
    Parses the file, strips column whitespace, then calls the right handler.
    """
    if file_type not in HANDLERS:
        raise ValueError(f"Unknown file type: {file_type}")

    # Parse — works for both CSV and Excel
    try:
        df = parse_file(content, filename)
    except Exception as e:
        raise ValueError(f"Could not parse file: {e}")

    # Strip accidental whitespace from column names
    df.columns = [c.strip() for c in df.columns]

    handler = HANDLERS[file_type]

    # Trades gets an extra argument (source tag)
    if file_type == "trades":
        return handler(df, db, source=source or "unknown")
    return handler(df, db)
