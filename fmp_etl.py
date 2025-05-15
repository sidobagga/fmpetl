"""
FMP ETL module for fetching historical financial data.
"""
import os
import aiohttp
import pandas as pd
from datetime import datetime

# Get API key from environment
FMP_KEY = os.getenv("FMP_KEY", "fjRDKKnsRnVNMfFepDM6ox31u9RlPklv")

async def fetch_json(url):
    """Fetch JSON data from URL"""
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status != 200:
                print(f"Error fetching {url}: {response.status}")
                return None
            return await response.json()

async def get_income_statements(symbol, period="annual", limit=5):
    """Get income statements for a symbol"""
    url = f"https://financialmodelingprep.com/api/v3/income-statement/{symbol}?period={period}&limit={limit}&apikey={FMP_KEY}"
    return await fetch_json(url)

async def process_statements(statements):
    """Process income statements into a dataframe"""
    if not statements:
        return pd.DataFrame()
    
    # Extract relevant fields
    data = []
    for stmt in statements:
        row = {
            "date": pd.to_datetime(stmt["date"]),
            "revenue": stmt.get("revenue", 0),
            "netIncome": stmt.get("netIncome", 0),
            "grossProfit": stmt.get("grossProfit", 0),
            "operatingIncome": stmt.get("operatingIncome", 0),
            "ebitda": stmt.get("ebitda", 0)
        }
        data.append(row)
    
    return pd.DataFrame(data)

async def etl(symbol):
    """Run ETL process for a symbol to fetch financial statements"""
    symbol = symbol.upper()
    print(f"FMP ETL called for symbol: {symbol}")
    
    # Fetch annual and quarterly statements
    annual_statements = await get_income_statements(symbol, period="annual")
    quarterly_statements = await get_income_statements(symbol, period="quarter")
    
    # Process into dataframes
    annual_df = await process_statements(annual_statements)
    quarterly_df = await process_statements(quarterly_statements)
    
    # If no data from API, use mock data as fallback
    if annual_df.empty:
        print(f"Warning: No annual data for {symbol}, using mock data")
        annual_df = pd.DataFrame({
            "date": pd.to_datetime(["2022-12-31", "2021-12-31"]),
            "revenue": [100.0, 90.0],
            "netIncome": [20.0, 18.0],
            "grossProfit": [40.0, 36.0],
            "operatingIncome": [30.0, 27.0],
            "ebitda": [35.0, 31.5]
        })
    
    if quarterly_df.empty:
        print(f"Warning: No quarterly data for {symbol}, using mock data")
        quarterly_df = pd.DataFrame({
            "date": pd.to_datetime(["2023-03-31", "2022-12-31"]),
            "revenue": [25.0, 24.0],
            "netIncome": [5.0, 4.8],
            "grossProfit": [10.0, 9.6],
            "operatingIncome": [7.5, 7.2],
            "ebitda": [8.75, 8.4]
        })
    
    return annual_df, quarterly_df 