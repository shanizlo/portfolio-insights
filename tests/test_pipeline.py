"""
Unit tests for the ingestion pipeline.
Tests each file type's validation, quality checks, and DB writes.
"""
import io
import pytest
import pandas as pd
from datetime import date

from app.pipeline import parse_file, to_dates, run_pipeline
from app.db import Customer, Stock, FxRate, Trade, HoldingSnapshot, DiscountRule


# ── Helpers ──────────────────────────────────────────────────────────────────

def csv(rows: list[dict]) -> bytes:
    """Build a CSV file from a list of row dicts."""
    return pd.DataFrame(rows).to_csv(index=False).encode()


def seed(db):
    """Insert the minimum reference data needed for fact table tests."""
    db.add(Customer(customer_id="C001", name="Alice", join_date=date(2020,1,1), tenure_years=4.0, segment="Retail"))
    db.add(Stock(ticker="AAPL", company_name="Apple", exchange="NASDAQ", currency="USD", sector="Tech", country="US"))
    db.add(FxRate(currency="GBP", rate_date=date(2025,1,1), usd_per_unit=1.28))
    db.commit()


# ── parse_file ────────────────────────────────────────────────────────────────

def test_parse_csv():
    df = parse_file(b"a,b\n1,2\n3,4\n", "test.csv")
    assert list(df.columns) == ["a", "b"]
    assert len(df) == 2

def test_parse_excel():
    buf = io.BytesIO()
    pd.DataFrame({"x": [1], "y": [2]}).to_excel(buf, index=False)
    df = parse_file(buf.getvalue(), "test.xlsx")
    assert list(df.columns) == ["x", "y"]

def test_parse_bad_file_raises():
    with pytest.raises(Exception):
        parse_file(b"\x00\x01\x02bad", "test.xlsx")


# ── to_dates ──────────────────────────────────────────────────────────────────

def test_to_dates_iso():
    df = pd.DataFrame({"d": ["2025-03-15"]})
    df = to_dates(df, ["d"])
    assert df["d"][0] == date(2025, 3, 15)

def test_to_dates_invalid_becomes_none():
    df = pd.DataFrame({"d": ["not-a-date"]})
    df = to_dates(df, ["d"])
    assert df["d"][0] is None


# ── Schema validation ─────────────────────────────────────────────────────────

def test_missing_column_raises(db):
    # trades file missing required columns
    bad_csv = csv([{"Customer_ID": "C001", "tradeDate": "2025-01-01"}])
    with pytest.raises(ValueError, match="Missing required columns"):
        run_pipeline("trades", bad_csv, "trades.csv", db)


# ── Stocks (reference — full replace) ────────────────────────────────────────

def test_stocks_insert(db):
    data = csv([{"Ticker": "AAPL", "CompanyName": "Apple", "Exchange": "NASDAQ",
                 "Currency": "USD", "Sector": "Tech", "Country": "US"}])
    result = run_pipeline("stocks", data, "stocks.csv", db)
    assert result["processed"] == 1
    assert db.query(Stock).count() == 1

def test_stocks_full_replace(db):
    # Upload 2 stocks
    run_pipeline("stocks", csv([
        {"Ticker": "AAPL", "CompanyName": "Apple", "Exchange": "NASDAQ", "Currency": "USD", "Sector": "Tech", "Country": "US"},
        {"Ticker": "GOOG", "CompanyName": "Alphabet", "Exchange": "NASDAQ", "Currency": "USD", "Sector": "Tech", "Country": "US"},
    ]), "stocks.csv", db)
    assert db.query(Stock).count() == 2

    # Re-upload with only 1 — old ones should be gone
    run_pipeline("stocks", csv([
        {"Ticker": "MSFT", "CompanyName": "Microsoft", "Exchange": "NASDAQ", "Currency": "USD", "Sector": "Tech", "Country": "US"},
    ]), "stocks.csv", db)
    assert db.query(Stock).count() == 1
    assert db.query(Stock).first().ticker == "MSFT"


# ── Customers (reference — full replace) ─────────────────────────────────────

def test_customers_full_replace(db):
    run_pipeline("customers", csv([
        {"CustomerID": "C001", "CustomerName": "Alice", "JoinDate": "2020-01-01", "TenureYears": 4.0, "Segment": "Retail"},
        {"CustomerID": "C002", "CustomerName": "Bob",   "JoinDate": "2022-01-01", "TenureYears": 2.0, "Segment": "Retail"},
    ]), "customers.csv", db)
    assert db.query(Customer).count() == 2

    run_pipeline("customers", csv([
        {"CustomerID": "C003", "CustomerName": "Carol", "JoinDate": "2023-01-01", "TenureYears": 1.0, "Segment": "VIP"},
    ]), "customers.csv", db)
    # Old customers deleted, only Carol remains
    assert db.query(Customer).count() == 1
    assert db.query(Customer).first().name == "Carol"


# ── Trades (fact — append only) ───────────────────────────────────────────────

def test_trades_valid_insert(db):
    seed(db)
    data = csv([{"Customer_ID": "C001", "tradeDate": "2025-01-01", "ticker": "AAPL",
                 "Side": "BUY", "Quantity": 100, "Px": 150.0, "TradeCurrency": "USD", "FeeUSD": 5.0}])
    result = run_pipeline("trades", data, "trades.csv", db, source="test")
    assert result["processed"] == 1
    assert result["errors"] == 0

def test_trades_negative_buy_rejected(db):
    seed(db)
    data = csv([{"Customer_ID": "C001", "tradeDate": "2025-01-02", "ticker": "AAPL",
                 "Side": "BUY", "Quantity": -100, "Px": 150.0, "TradeCurrency": "USD", "FeeUSD": ""}])
    result = run_pipeline("trades", data, "trades.csv", db, source="test")
    assert result["errors"] == 1
    assert result["processed"] == 0

def test_trades_duplicate_skipped(db):
    seed(db)
    data = csv([{"Customer_ID": "C001", "tradeDate": "2025-01-03", "ticker": "AAPL",
                 "Side": "BUY", "Quantity": 50, "Px": 155.0, "TradeCurrency": "USD", "FeeUSD": ""}])
    run_pipeline("trades", data, "trades.csv", db, source="test")
    run_pipeline("trades", data, "trades.csv", db, source="test")  # same file again
    assert db.query(Trade).filter_by(trade_date=date(2025, 1, 3)).count() == 1  # not doubled

def test_trades_unknown_ticker_warns(db):
    seed(db)
    data = csv([{"Customer_ID": "C001", "tradeDate": "2025-01-04", "ticker": "FAKE",
                 "Side": "BUY", "Quantity": 10, "Px": 50.0, "TradeCurrency": "USD", "FeeUSD": ""}])
    result = run_pipeline("trades", data, "trades.csv", db, source="test")
    assert result["warnings"] >= 1   # warned but still written

def test_trades_missing_fee_stored_as_null(db):
    seed(db)
    data = csv([{"Customer_ID": "C001", "tradeDate": "2025-01-05", "ticker": "AAPL",
                 "Side": "BUY", "Quantity": 10, "Px": 150.0, "TradeCurrency": "USD", "FeeUSD": ""}])
    run_pipeline("trades", data, "trades.csv", db, source="test")
    trade = db.query(Trade).filter_by(trade_date=date(2025, 1, 5)).first()
    assert trade.fee_usd is None   # null, not zero


# ── Discount rules ────────────────────────────────────────────────────────────

def test_discount_rules_inserted(db):
    data = csv([
        {"MinPortfolioValueFromUSD": 0,     "MinTenureYears": 0, "BaseDiscountPct": 0,    "TenureBonusPct": 0},
        {"MinPortfolioValueFromUSD": 50000, "MinTenureYears": 1, "BaseDiscountPct": 0.02, "TenureBonusPct": 0.01},
    ])
    result = run_pipeline("discount-rules", data, "rules.csv", db)
    assert result["processed"] == 2
    assert db.query(DiscountRule).count() == 2
