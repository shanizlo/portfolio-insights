"""
Integration tests for all API endpoints.
Uses FastAPI's TestClient — no running server needed.
"""
import pytest
from datetime import date
from app.db import Customer, Stock, FxRate, PriceHistory, HoldingSnapshot, DiscountRule

HEADERS = {"x-api-key": "dev-internal-key"}


def seed(db):
    """Seed the minimum data needed to test query endpoints."""
    db.add(Customer(customer_id="C001", name="Alice", join_date=date(2020,1,1), tenure_years=4.5, segment="Retail"))
    db.add(Stock(ticker="AAPL", company_name="Apple", exchange="NASDAQ", currency="USD", sector="Technology", country="US"))
    db.add(FxRate(currency="USD", rate_date=date(2025,6,1), usd_per_unit=1.0))
    db.add(PriceHistory(ticker="AAPL", price_date=date(2025,6,1), close=200.0, currency="USD", close_usd=200.0))
    db.add(HoldingSnapshot(customer_id="C001", ticker="AAPL", quantity=100, as_of_date=date(2025,6,1)))
    db.add(DiscountRule(min_portfolio_value_usd=0,     min_tenure_years=0, base_discount_pct=0,    tenure_bonus_pct=0))
    db.add(DiscountRule(min_portfolio_value_usd=50000, min_tenure_years=1, base_discount_pct=0.02, tenure_bonus_pct=0.01))
    db.commit()


# ── Auth ──────────────────────────────────────────────────────────────────────

def test_wrong_api_key_rejected(client):
    res = client.post("/ingest/stocks", headers={"x-api-key": "wrong"},
                      files={"file": ("s.csv", b"a,b", "text/csv")})
    assert res.status_code == 403

def test_missing_api_key_rejected(client):
    res = client.post("/ingest/stocks", files={"file": ("s.csv", b"a,b", "text/csv")})
    assert res.status_code == 422  # header missing entirely


# ── Health ────────────────────────────────────────────────────────────────────

def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


# ── Ingest ────────────────────────────────────────────────────────────────────

def test_ingest_returns_job_id(client):
    import pandas as pd, io
    csv = pd.DataFrame([{"Ticker": "AAPL", "CompanyName": "Apple", "Exchange": "NASDAQ",
                          "Currency": "USD", "Sector": "Tech", "Country": "US"}]).to_csv(index=False).encode()
    res = client.post("/ingest/stocks", headers=HEADERS, files={"file": ("stocks.csv", csv, "text/csv")})
    assert res.status_code == 200
    assert "job_id" in res.json()
    assert res.json()["status"] == "pending"

def test_ingest_status_pending(client):
    import pandas as pd
    csv = pd.DataFrame([{"Ticker": "X", "CompanyName": "Y", "Exchange": "Z",
                          "Currency": "USD", "Sector": "T", "Country": "US"}]).to_csv(index=False).encode()
    job_id = client.post("/ingest/stocks", headers=HEADERS,
                         files={"file": ("s.csv", csv, "text/csv")}).json()["job_id"]
    status = client.get(f"/ingest/status/{job_id}", headers=HEADERS).json()
    assert status["job_id"] == job_id

def test_ingest_status_404(client):
    assert client.get("/ingest/status/does-not-exist", headers=HEADERS).status_code == 404


# ── Summary ───────────────────────────────────────────────────────────────────

def test_summary(client, db):
    seed(db)
    data = client.get("/summary").json()
    assert data["total_aum_usd"] == 20000.0   # 100 shares × $200
    assert data["customer_count"] >= 1

def test_summary_by_sector(client, db):
    seed(db)
    sectors = client.get("/summary/by-sector").json()
    assert any(s["sector"] == "Technology" for s in sectors)


# ── Customers ─────────────────────────────────────────────────────────────────

def test_list_customers(client, db):
    seed(db)
    res = client.get("/customers").json()
    assert any(c["customer_id"] == "C001" for c in res)

def test_get_customer(client, db):
    seed(db)
    assert client.get("/customers/C001").json()["name"] == "Alice"

def test_get_customer_not_found(client):
    assert client.get("/customers/NOBODY").status_code == 404


# ── Holdings ──────────────────────────────────────────────────────────────────

def test_holdings_value(client, db):
    seed(db)
    data = client.get("/customers/C001/holdings").json()
    assert data["total_value_usd"] == 20000.0   # 100 × $200
    assert data["holdings"][0]["pct_of_portfolio"] == 100.0

def test_holdings_empty_customer(client, db):
    seed(db)
    # Customer exists but has no holdings on a different date
    data = client.get("/customers/C001/holdings?as_of=2000-01-01").json()
    assert data["holdings"] == []


# ── Discount ──────────────────────────────────────────────────────────────────

def test_discount(client, db):
    seed(db)
    data = client.get("/customers/C001/discount").json()
    assert "total_discount_pct" in data
    assert data["portfolio_value_usd"] == 20000.0

def test_discount_customer_not_found(client):
    assert client.get("/customers/NOBODY/discount").status_code == 404


# ── Instruments ───────────────────────────────────────────────────────────────

def test_search_instruments(client, db):
    seed(db)
    res = client.get("/instruments?search=AAPL").json()
    assert any(i["ticker"] == "AAPL" for i in res)

def test_instrument_prices(client, db):
    seed(db)
    prices = client.get("/instruments/AAPL/prices?from_date=2025-01-01&to_date=2025-12-31").json()
    assert len(prices) == 1
    assert prices[0]["close"] == 200.0


# ── Trades ────────────────────────────────────────────────────────────────────

def test_trades_empty(client, db):
    seed(db)
    data = client.get("/customers/C001/trades").json()
    assert data["total"] == 0
    assert data["trades"] == []
