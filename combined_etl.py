import os, asyncio
from datetime import date
import pandas as pd
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import insert, select
from models import metadata, symbols, financial_periods, financial_metrics, \
                   analyst_estimates, peer_operating, peer_trading, price_targets

# Default to SQLite for development/testing if no DB_URL is provided
DB_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///:memory:")
FMP_KEY = os.getenv("FMP_KEY", "fjRDKKnsRnVNMfFepDM6ox31u9RlPklv")

# Create engine
engine = create_async_engine(DB_URL, future=True)

from fmp_etl import etl as run_core_etl              # ← previous script #1
from fmp_forward_and_comp_peers import main as run_fwd_etl  # ← previous script #2

# Check if we're using SQLite (for testing)
IS_SQLITE = "sqlite" in DB_URL

# ───────────────────────────────────────────────────────────────────────────
async def upsert_core(symbol: str, df_a: pd.DataFrame, df_q: pd.DataFrame, session: AsyncSession):
    # ensure symbol row
    sym_id = await get_or_create_symbol(symbol, session)
    for period_type, df in (("annual", df_a), ("quarter", df_q)):
        for _, row in df.iterrows():
            period_end = row["date"].date()
            
            # Check if period exists
            existing = await session.execute(
                select(financial_periods.c.id)
                .where(financial_periods.c.symbol_id == sym_id,
                       financial_periods.c.period_type == period_type,
                       financial_periods.c.period_end == period_end)
            )
            period_row = existing.first()
            
            if period_row:
                # Period exists, get its ID
                period_id = period_row[0]
            else:
                # Period doesn't exist, insert it
                stmt = insert(financial_periods).values(
                    symbol_id=sym_id, period_type=period_type, period_end=period_end
                )
                result = await session.execute(stmt)
                
                # Get last inserted ID for SQLite or get result for PostgreSQL
                if IS_SQLITE:
                    # Get the last inserted row ID
                    res = await session.execute(select(financial_periods.c.id).order_by(financial_periods.c.id.desc()).limit(1))
                    period_id = res.scalar_one()
                else:
                    # For PostgreSQL, use RETURNING
                    period_id = result.inserted_primary_key[0]
            
            # insert metrics (delete old ones first if they exist)
            if period_id:
                # Insert new metrics
                metric_rows = [
                    dict(period_id=period_id, metric_code=col, value=row[col])
                    for col in df.columns if col not in ("date",)
                ]
                await session.execute(insert(financial_metrics), metric_rows)

async def get_or_create_symbol(ticker: str, session: AsyncSession) -> int:
    res = await session.execute(select(symbols.c.id).where(symbols.c.ticker == ticker))
    row = res.first()
    if row:
        return row[0]  # Access the ID directly from the row tuple
    
    # Insert new symbol
    stmt = insert(symbols).values(ticker=ticker)
    result = await session.execute(stmt)
    
    # Get the ID based on the database type
    if IS_SQLITE:
        # Get the last inserted row ID for SQLite
        res = await session.execute(select(symbols.c.id).order_by(symbols.c.id.desc()).limit(1))
        return res.scalar_one()
    else:
        # For PostgreSQL, use the result's primary key
        return result.inserted_primary_key[0]

async def combined_etl(symbol: str, peers: list[str]):
    """Run both ETLs and load database."""
    # 1) run historical core ETL – returns dataframes
    df_a, df_q = await run_core_etl(symbol)          # modify run_core_etl to return dfs
    # 2) run forward & peers ETL – writes parquet; returns dfs
    est_df, oper_df, trading_df, pt_df = await run_fwd_etl(symbol, peers)  # adapt return

    # Use the imported engine - don't create a new one
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)

    async with AsyncSession(engine) as session:
        await upsert_core(symbol, df_a, df_q, session)

        sym_id = await get_or_create_symbol(symbol, session)

        # analyst estimates - check for existing and insert if not found
        for _, r in est_df.iterrows():
            fiscal_year = int(r.year)
            # Check if exists
            existing = await session.execute(
                select(analyst_estimates.c.id)
                .where(analyst_estimates.c.symbol_id == sym_id,
                       analyst_estimates.c.fiscal_year == fiscal_year)
            )
            if not existing.first():
                est_row = dict(
                    symbol_id=sym_id, 
                    fiscal_year=fiscal_year, 
                    payload=r.drop("year").to_dict()
                )
                await session.execute(insert(analyst_estimates), [est_row])

        # peers operating & trading - check for existing and insert if not found
        for df, table in ((oper_df, peer_operating), (trading_df, peer_trading)):
            for _, r in df.iterrows():
                peer_ticker = r["symbol"]
                # Check if exists
                existing = await session.execute(
                    select(table.c.id)
                    .where(table.c.host_symbol_id == sym_id,
                           table.c.peer_ticker == peer_ticker)
                )
                if not existing.first():
                    row = dict(
                        host_symbol_id=sym_id, 
                        peer_ticker=peer_ticker, 
                        payload=r.drop("symbol").to_dict()
                    )
                    await session.execute(insert(table), [row])

        # price targets - just insert directly
        pt_rows = [
            dict(symbol_id=sym_id,
                 broker=r["broker"], analyst=r["analyst"],
                 research_date=r["publishedDate"], target_px=r["priceTarget"],
                 direction="up" if r.get("direction")=="up" else
                           "down" if r.get("direction")=="down" else "same")
            for _, r in pt_df.iterrows() if r["analyst"]!="SUMMARY"
        ]
        await session.execute(insert(price_targets), pt_rows)

        await session.commit()
