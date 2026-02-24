"""
Portfolio Insights Platform — main application file.

All routes live here (ingest + query) to keep things easy to navigate.
The app starts a background worker on startup that processes uploaded files.

Auth: every request must include the header  x-api-key: dev-internal-key
"""
import asyncio
import logging
import os
import uuid
from datetime import date, datetime, timedelta

from fastapi import FastAPI, UploadFile, File, Depends, HTTPException, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import func, desc
from sqlalchemy.orm import Session

from app.db import (
    get_db, init_db,
    Customer, Stock, Trade, HoldingSnapshot,
    PriceHistory, FxRate, DiscountRule, IngestionJob
)
from app.worker import queue, worker_loop

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")

API_KEY = os.environ.get("API_KEY", "dev-internal-key")

app = FastAPI(title="Portfolio Insights Platform", version="1.0.0")

# Allow the frontend (served on port 3000) to call the API (on port 8000)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.on_event("startup")
async def startup():
    init_db()
    asyncio.create_task(worker_loop())  # start the background worker


# ── Auth ─────────────────────────────────────────────────────────────────────

def require_api_key(x_api_key: str = Header(...)):
    """Dependency — rejects requests with a wrong or missing API key."""
    if x_api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")


# ── Helper ───────────────────────────────────────────────────────────────────

def get_fx_rate(db: Session, currency: str, on_date: date) -> float:
    """Get USD exchange rate for a currency on a given date (nearest prior date)."""
    if currency == "USD":
        return 1.0
    rate = (
        db.query(FxRate)
        .filter(FxRate.currency == currency, FxRate.rate_date <= on_date)
        .order_by(desc(FxRate.rate_date))
        .first()
    )
    return float(rate.usd_per_unit) if rate else 1.0


# ═════════════════════════════════════════════════════════════════════════════
# INGEST ENDPOINTS
# Each one accepts a file upload, creates a job record, and queues it
# for the background worker. Returns a job_id for polling.
# ═════════════════════════════════════════════════════════════════════════════

async def enqueue(file_type: str, file: UploadFile, db: Session, source: str = None):
    """Shared logic for all ingest endpoints."""
    content = await file.read()
    job_id  = str(uuid.uuid4())

    # Create a "pending" job record so the caller can poll for the result
    db.add(IngestionJob(id=job_id, file_type=file_type, created_at=datetime.utcnow()))
    db.commit()

    # Put the job on the queue — the worker picks it up and processes it
    await queue.put({"job_id": job_id, "file_type": file_type,
                     "content": content, "filename": file.filename, "source": source})

    return {"job_id": job_id, "status": "pending"}


@app.post("/ingest/stocks")
async def ingest_stocks(file: UploadFile = File(...), db: Session = Depends(get_db), _=Depends(require_api_key)):
    return await enqueue("stocks", file, db)

@app.post("/ingest/customers")
async def ingest_customers(file: UploadFile = File(...), db: Session = Depends(get_db), _=Depends(require_api_key)):
    return await enqueue("customers", file, db)

@app.post("/ingest/account-map")
async def ingest_account_map(file: UploadFile = File(...), db: Session = Depends(get_db), _=Depends(require_api_key)):
    return await enqueue("account-map", file, db)

@app.post("/ingest/discount-rules")
async def ingest_discount_rules(file: UploadFile = File(...), db: Session = Depends(get_db), _=Depends(require_api_key)):
    return await enqueue("discount-rules", file, db)

@app.post("/ingest/fx-rates")
async def ingest_fx_rates(file: UploadFile = File(...), db: Session = Depends(get_db), _=Depends(require_api_key)):
    return await enqueue("fx-rates", file, db)

@app.post("/ingest/prices")
async def ingest_prices(file: UploadFile = File(...), db: Session = Depends(get_db), _=Depends(require_api_key)):
    return await enqueue("prices", file, db)

@app.post("/ingest/holdings")
async def ingest_holdings(file: UploadFile = File(...), db: Session = Depends(get_db), _=Depends(require_api_key)):
    return await enqueue("holdings", file, db)

@app.post("/ingest/trades")
async def ingest_trades(file: UploadFile = File(...), source: str = "unknown",
                        db: Session = Depends(get_db), _=Depends(require_api_key)):
    return await enqueue("trades", file, db, source=source)

@app.get("/ingest/status/{job_id}")
def job_status(job_id: str, db: Session = Depends(get_db), _=Depends(require_api_key)):
    """Poll this endpoint after uploading a file to see if processing is complete."""
    job = db.query(IngestionJob).filter_by(id=job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {
        "job_id":   job.id,
        "file_type": job.file_type,
        "status":   job.status,
        "records_processed": job.records_processed,
        "warning_count":     job.warning_count,
        "error_count":       job.error_count,
        "errors":            job.error_detail or [],
        "created_at":        job.created_at,
        "completed_at":      job.completed_at,
    }


# ═════════════════════════════════════════════════════════════════════════════
# QUERY ENDPOINTS
# All read-only. Values always returned in USD.
# ═════════════════════════════════════════════════════════════════════════════

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/summary")
def summary(db: Session = Depends(get_db)):
    """Dashboard totals: AUM, customer count, trade count, data freshness."""
    latest_date = db.query(func.max(HoldingSnapshot.as_of_date)).scalar()
    aum = 0.0

    if latest_date:
        # Join holdings with nearest prior price to compute value
        ph_sub = (
            db.query(PriceHistory.ticker.label("t"), func.max(PriceHistory.price_date).label("d"))
            .filter(PriceHistory.price_date <= latest_date)
            .group_by(PriceHistory.ticker)
        ).subquery()
        rows = (
            db.query(HoldingSnapshot, PriceHistory)
            .join(ph_sub, ph_sub.c.t == HoldingSnapshot.ticker)
            .join(PriceHistory, (PriceHistory.ticker == HoldingSnapshot.ticker) &
                                 (PriceHistory.price_date == ph_sub.c.d))
            .filter(HoldingSnapshot.as_of_date == latest_date)
            .all()
        )
        for h, p in rows:
            fx = get_fx_rate(db, p.currency, latest_date)
            aum += h.quantity * float(p.close) * fx

    # Compute trade count in the last 30 days relative to the latest trade date
    latest_trade_date = db.query(func.max(Trade.trade_date)).scalar()
    thirty_day_count = 0
    if latest_trade_date:
        start_date = latest_trade_date - timedelta(days=30)
        thirty_day_count = db.query(func.count(Trade.id)).filter(
            Trade.trade_date >= start_date
        ).scalar()

    return {
        "total_aum_usd":       round(aum, 2),
        "customer_count":      db.query(func.count(Customer.customer_id)).scalar(),
        "trade_count_30d":     thirty_day_count,
        "latest_snapshot_date": latest_date,
        "data_freshness": {
            "trades": str(db.query(func.max(Trade.trade_date)).scalar() or ""),
            "prices": str(db.query(func.max(PriceHistory.price_date)).scalar() or ""),
            "fx_rates": str(db.query(func.max(FxRate.rate_date)).scalar() or ""),
        }
    }


@app.get("/summary/by-sector")
def summary_by_sector(db: Session = Depends(get_db)):
    """AUM broken down by sector, using the latest holdings snapshot."""
    latest_date = db.query(func.max(HoldingSnapshot.as_of_date)).scalar()
    if not latest_date:
        return []

    ph_sub = (
        db.query(PriceHistory.ticker.label("t"), func.max(PriceHistory.price_date).label("d"))
        .filter(PriceHistory.price_date <= latest_date)
        .group_by(PriceHistory.ticker)
    ).subquery()

    rows = (
        db.query(HoldingSnapshot, PriceHistory, Stock)
        .join(ph_sub, ph_sub.c.t == HoldingSnapshot.ticker)
        .join(PriceHistory, (PriceHistory.ticker == HoldingSnapshot.ticker) &
                             (PriceHistory.price_date == ph_sub.c.d))
        .join(Stock, Stock.ticker == HoldingSnapshot.ticker)
        .filter(HoldingSnapshot.as_of_date == latest_date)
        .all()
    )

    by_sector: dict[str, float] = {}
    for h, p, s in rows:
        fx    = get_fx_rate(db, p.currency, latest_date)
        value = h.quantity * float(p.close) * fx
        key   = s.sector or "Unknown"
        by_sector[key] = by_sector.get(key, 0) + value

    return [{"sector": k, "aum_usd": round(v, 2)}
            for k, v in sorted(by_sector.items(), key=lambda x: -x[1])]


@app.get("/customers")
def list_customers(limit: int = 50, offset: int = 0, db: Session = Depends(get_db)):
    customers = db.query(Customer).offset(offset).limit(limit).all()
    return [{"customer_id": c.customer_id, "name": c.name,
             "segment": c.segment, "tenure_years": float(c.tenure_years or 0)}
            for c in customers]


@app.get("/customers/{customer_id}")
def get_customer(customer_id: str, db: Session = Depends(get_db)):
    c = db.query(Customer).filter_by(customer_id=customer_id).first()
    if not c:
        raise HTTPException(status_code=404, detail="Customer not found")
    return {"customer_id": c.customer_id, "name": c.name, "segment": c.segment,
            "join_date": c.join_date, "tenure_years": float(c.tenure_years or 0)}


@app.get("/customers/{customer_id}/holdings")
def get_holdings(customer_id: str, as_of: date = None, db: Session = Depends(get_db)):
    """
    Returns all holdings for a customer on a given date.
    Defaults to the latest snapshot date if none is provided.
    Value is quantity × close price × FX rate (all in USD).
    """
    if not as_of:
        as_of = db.query(func.max(HoldingSnapshot.as_of_date)).scalar()
    if not as_of:
        return {"as_of": None, "total_value_usd": 0, "holdings": []}

    ph_sub = (
        db.query(PriceHistory.ticker.label("t"), func.max(PriceHistory.price_date).label("d"))
        .filter(PriceHistory.price_date <= as_of)
        .group_by(PriceHistory.ticker)
    ).subquery()

    rows = (
        db.query(HoldingSnapshot, PriceHistory, Stock)
        .join(ph_sub, ph_sub.c.t == HoldingSnapshot.ticker)
        .join(PriceHistory, (PriceHistory.ticker == HoldingSnapshot.ticker) &
                             (PriceHistory.price_date == ph_sub.c.d))
        .join(Stock, Stock.ticker == HoldingSnapshot.ticker)
        .filter(HoldingSnapshot.customer_id == customer_id, HoldingSnapshot.as_of_date == as_of)
        .all()
    )

    holdings = []
    total_usd = 0.0
    for h, p, s in rows:
        fx        = get_fx_rate(db, p.currency, as_of)
        value_usd = h.quantity * float(p.close) * fx
        total_usd += value_usd
        holdings.append({
            "ticker":       h.ticker,
            "company_name": s.company_name,
            "sector":       s.sector,
            "quantity":     h.quantity,
            "close_price":  float(p.close),
            "currency":     p.currency,
            "value_usd":    round(value_usd, 2),
        })

    # Add each holding's share of the total portfolio
    for h in holdings:
        h["pct_of_portfolio"] = round(h["value_usd"] / total_usd * 100, 2) if total_usd else 0

    return {"as_of": as_of, "total_value_usd": round(total_usd, 2), "holdings": holdings}


@app.get("/customers/{customer_id}/portfolio-value")
def portfolio_value_over_time(customer_id: str, from_date: date = Query(...),
                               to_date: date = Query(...), db: Session = Depends(get_db)):
    """Daily portfolio value for a customer over a date range (for the chart)."""
    # Get all snapshot dates in the range for this customer
    dates = (
        db.query(HoldingSnapshot.as_of_date)
        .filter(HoldingSnapshot.customer_id == customer_id,
                HoldingSnapshot.as_of_date >= from_date,
                HoldingSnapshot.as_of_date <= to_date)
        .distinct().order_by(HoldingSnapshot.as_of_date).all()
    )

    result = []
    for (snap_date,) in dates:
        ph_sub = (
            db.query(PriceHistory.ticker.label("t"), func.max(PriceHistory.price_date).label("d"))
            .filter(PriceHistory.price_date <= snap_date)
            .group_by(PriceHistory.ticker)
        ).subquery()
        rows = (
            db.query(HoldingSnapshot, PriceHistory)
            .join(ph_sub, ph_sub.c.t == HoldingSnapshot.ticker)
            .join(PriceHistory, (PriceHistory.ticker == HoldingSnapshot.ticker) &
                                 (PriceHistory.price_date == ph_sub.c.d))
            .filter(HoldingSnapshot.customer_id == customer_id,
                    HoldingSnapshot.as_of_date == snap_date)
            .all()
        )
        total = sum(h.quantity * float(p.close) * get_fx_rate(db, p.currency, snap_date)
                    for h, p in rows)
        result.append({"date": snap_date, "value_usd": round(total, 2)})

    return result


@app.get("/customers/{customer_id}/trades")
def get_trades(customer_id: str,
               from_date: date = None, to_date: date = None,
               side: str = None, ticker: str = None,
               limit: int = 50, offset: int = 0,
               db: Session = Depends(get_db)):
    """Trade history with optional filters."""
    q = db.query(Trade).filter(Trade.customer_id == customer_id)
    if from_date: q = q.filter(Trade.trade_date >= from_date)
    if to_date:   q = q.filter(Trade.trade_date <= to_date)
    if side:      q = q.filter(Trade.side == side.upper())
    if ticker:    q = q.filter(Trade.ticker == ticker.upper())

    total  = q.count()
    trades = q.order_by(desc(Trade.trade_date)).offset(offset).limit(limit).all()

    return {
        "total": total,
        "trades": [{
            "trade_date":     t.trade_date,
            "ticker":         t.ticker,
            "side":           t.side,
            "quantity":       t.quantity,
            "price":          float(t.price),
            "trade_currency": t.trade_currency,
            "price_usd":      float(t.price_usd) if t.price_usd else None,
            "fee_usd":        float(t.fee_usd)   if t.fee_usd   else None,
            "source":         t.source,
        } for t in trades]
    }


@app.get("/customers/{customer_id}/discount")
def get_discount(customer_id: str, db: Session = Depends(get_db)):
    """
    Returns the applicable fee discount for a customer.
    Looks up the best matching tier in discount_rules based on
    portfolio value and tenure (both must meet the minimums).
    """
    c = db.query(Customer).filter_by(customer_id=customer_id).first()
    if not c:
        raise HTTPException(status_code=404, detail="Customer not found")

    # Calculate current portfolio value
    latest_date = db.query(func.max(HoldingSnapshot.as_of_date)).filter(
        HoldingSnapshot.customer_id == customer_id).scalar()

    portfolio_value = 0.0
    if latest_date:
        rows = (
            db.query(HoldingSnapshot, PriceHistory)
            .join(PriceHistory, (PriceHistory.ticker == HoldingSnapshot.ticker) &
                                (PriceHistory.price_date == latest_date))
            .filter(HoldingSnapshot.customer_id == customer_id,
                    HoldingSnapshot.as_of_date == latest_date)
            .all()
        )
        portfolio_value = sum(h.quantity * float(p.close) * get_fx_rate(db, p.currency, latest_date)
                              for h, p in rows)

    tenure = float(c.tenure_years or 0)

    # Best matching tier = highest value threshold the customer qualifies for
    rule = (
        db.query(DiscountRule)
        .filter(DiscountRule.min_portfolio_value_usd <= portfolio_value,
                DiscountRule.min_tenure_years <= tenure)
        .order_by(desc(DiscountRule.min_portfolio_value_usd), desc(DiscountRule.min_tenure_years))
        .first()
    )

    base  = float(rule.base_discount_pct)  if rule else 0.0
    bonus = float(rule.tenure_bonus_pct)   if rule else 0.0

    return {
        "customer_id":        customer_id,
        "portfolio_value_usd": round(portfolio_value, 2),
        "tenure_years":        tenure,
        "base_discount_pct":   base,
        "tenure_bonus_pct":    bonus,
        "total_discount_pct":  round(base + bonus, 4),
    }


@app.get("/instruments")
def list_instruments(search: str = None, limit: int = 50, db: Session = Depends(get_db)):
    q = db.query(Stock)
    if search:
        q = q.filter(Stock.ticker.ilike(f"%{search}%") | Stock.company_name.ilike(f"%{search}%"))
    stocks = q.limit(limit).all()
    return [{"ticker": s.ticker, "company_name": s.company_name, "sector": s.sector,
             "exchange": s.exchange, "currency": s.currency, "country": s.country}
            for s in stocks]


@app.get("/instruments/{ticker}/prices")
def instrument_prices(ticker: str, from_date: date = Query(...),
                      to_date: date = Query(...), db: Session = Depends(get_db)):
    prices = (
        db.query(PriceHistory)
        .filter(PriceHistory.ticker == ticker,
                PriceHistory.price_date >= from_date,
                PriceHistory.price_date <= to_date)
        .order_by(PriceHistory.price_date).all()
    )
    return [{"date": p.price_date, "close": float(p.close), "currency": p.currency}
            for p in prices]
