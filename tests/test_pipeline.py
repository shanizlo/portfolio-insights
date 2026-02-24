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


def test_to_dates_basic_and_invalid():
    df = pd.DataFrame({"d": ["2025-03-15", "not-a-date"]})
    df = to_dates(df, ["d"])
    assert df["d"][0] == date(2025, 3, 15)
    assert pd.isna(df["d"][1]) or df["d"][1] is None


# ── Schema validation ─────────────────────────────────────────────────────────

def test_missing_column_raises(db):
    # trades file missing required columns
    bad_csv = csv([{"Customer_ID": "C001", "tradeDate": "2025-01-01"}])
    with pytest.raises(ValueError, match="Missing required columns"):
        run_pipeline("trades", bad_csv, "trades.csv", db)


# ── Stocks (reference — basic insert) ────────────────────────────────────────

def test_stocks_insert(db):
    data = csv([{"Ticker": "AAPL", "CompanyName": "Apple", "Exchange": "NASDAQ",
                 "Currency": "USD", "Sector": "Tech", "Country": "US"}])
    result = run_pipeline("stocks", data, "stocks.csv", db)
    assert result["processed"] == 1
    assert db.query(Stock).count() == 1


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


# ── Trades tests are omitted in POC to avoid dialect-specific ON CONFLICT


# ── Discount rules ────────────────────────────────────────────────────────────

def test_discount_rules_inserted(db):
    data = csv([
        {"MinPortfolioValueFromUSD": 0,     "MinTenureYears": 0, "BaseDiscountPct": 0,    "TenureBonusPct": 0},
        {"MinPortfolioValueFromUSD": 50000, "MinTenureYears": 1, "BaseDiscountPct": 0.02, "TenureBonusPct": 0.01},
    ])
    result = run_pipeline("discount-rules", data, "rules.csv", db)
    assert result["processed"] == 2
    assert db.query(DiscountRule).count() == 2
