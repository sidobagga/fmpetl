import os, asyncio
from fastapi import FastAPI, HTTPException, Query
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import select, text
from models import metadata, symbols, financial_metrics, financial_periods
from combined_etl import combined_etl

# Default to SQLite for development/testing if no DB_URL is provided
DB_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///:memory:")
FMP_KEY = os.getenv("FMP_KEY", "demo")
engine = create_async_engine(DB_URL, future=True)

app = FastAPI(title="Financial Data Service")

@app.on_event("startup")
async def start():
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)

# ---------- helpers -------------------------------------------------------
async def ensure_data(symbol: str):
    async with AsyncSession(engine) as session:
        res = await session.execute(select(symbols.c.id).where(symbols.c.ticker == symbol))
        if not res.first():
            # cache miss: kick off ETL (peers empty for now)
            await combined_etl(symbol, peers=[])
        # else: data already present

# ---------- endpoints -----------------------------------------------------
@app.get("/metrics/{symbol}")
async def get_metrics(
    symbol: str,
    period_type: str = Query("annual", pattern="^(annual|quarter)$"),
    metrics: list[str] = Query(...)
):
    symbol = symbol.upper()
    await ensure_data(symbol)

    async with AsyncSession(engine) as sess:
        # Check if using SQLite
        is_sqlite = "sqlite" in DB_URL.lower()
        
        if is_sqlite:
            # For SQLite, we'll use the SQL directly with metrics in the SQL text
            metrics_list = "', '".join(metrics)
            sql = text(f"""
            SELECT p.period_end, m.metric_code, m.value
            FROM financial_metrics m
            JOIN financial_periods p ON p.id = m.period_id
            JOIN symbols s ON s.id = p.symbol_id
            WHERE s.ticker = :sym
              AND p.period_type = :ptype
              AND m.metric_code IN ('{metrics_list}')
            ORDER BY p.period_end DESC;
            """)
            rows = (await sess.execute(sql, {"sym": symbol, "ptype": period_type})).all()
        else:
            # For PostgreSQL, use ANY
            sql = text("""
            SELECT p.period_end, m.metric_code, m.value
            FROM financial_metrics m
            JOIN financial_periods p ON p.id = m.period_id
            JOIN symbols s ON s.id = p.symbol_id
            WHERE s.ticker = :sym
              AND p.period_type = :ptype
              AND m.metric_code = ANY(:metrics)
            ORDER BY p.period_end DESC;
            """)
            rows = (await sess.execute(sql, dict(sym=symbol,
                                               ptype=period_type,
                                               metrics=metrics))).all()

    if not rows:
        raise HTTPException(404, "No data found")

    # reshape to { period_end: {metric: value} }
    data = {}
    for period_end, code, val in rows:
        data.setdefault(str(period_end), {})[code] = val
    return {"symbol": symbol, "period_type": period_type, "data": data}

@app.get("/peers/{symbol}/operating")
async def peers_operating(symbol:str):
    symbol = symbol.upper()
    await ensure_data(symbol)
    async with AsyncSession(engine) as sess:
        q = text("""
          SELECT payload
          FROM peer_operating po
          JOIN symbols s ON s.id = po.host_symbol_id
          WHERE s.ticker = :sym;
        """)
        rows = (await sess.execute(q, dict(sym=symbol))).scalars().all()
    return {"symbol": symbol, "peers": rows}

@app.get("/peers/{symbol}/trading")
async def peers_trading(symbol:str):
    symbol = symbol.upper()
    await ensure_data(symbol)
    async with AsyncSession(engine) as sess:
        q = text("""
          SELECT payload
          FROM peer_trading pt
          JOIN symbols s ON s.id = pt.host_symbol_id
          WHERE s.ticker = :sym;
        """)
        rows = (await sess.execute(q, dict(sym=symbol))).scalars().all()
    return {"symbol": symbol, "peers": rows}
