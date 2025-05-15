"""
FMP module for fetching forward-looking estimates and competitor data.
"""
import os
import aiohttp
import pandas as pd
from datetime import datetime, timedelta

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

async def get_peers(symbol):
    """Get peer companies for a symbol"""
    url = f"https://financialmodelingprep.com/api/v3/stock_peers?symbol={symbol}&apikey={FMP_KEY}"
    data = await fetch_json(url)
    if not data or not isinstance(data, list) or len(data) == 0:
        return []
    return data[0].get("peersList", [])

async def get_earnings_estimates(symbol):
    """Get earnings estimates for a symbol"""
    url = f"https://financialmodelingprep.com/api/v3/analyst-estimates/{symbol}?apikey={FMP_KEY}"
    return await fetch_json(url)

async def get_price_targets(symbol):
    """Get price targets for a symbol"""
    url = f"https://financialmodelingprep.com/api/v3/price-target?symbol={symbol}&apikey={FMP_KEY}"
    return await fetch_json(url)

async def get_key_metrics(symbol):
    """Get key metrics for a symbol"""
    url = f"https://financialmodelingprep.com/api/v3/key-metrics-ttm/{symbol}?apikey={FMP_KEY}"
    return await fetch_json(url)

async def process_estimates(estimates):
    """Process earnings estimates into a dataframe"""
    if not estimates:
        return pd.DataFrame()
    
    current_year = datetime.now().year
    next_year = current_year + 1
    
    # Extract relevant fields
    data = []
    for est in estimates:
        year = est.get("year")
        if year and (year == str(current_year) or year == str(next_year)):
            row = {
                "year": int(year),
                "revenue": est.get("estimatedRevenue", 0),
                "netIncome": est.get("estimatedNetIncome", 0),
                "ebitda": est.get("estimatedEBITDA", 0)
            }
            data.append(row)
    
    return pd.DataFrame(data)

async def main(symbol, peers=None):
    """Run ETL process for forward-looking estimates and peer data"""
    symbol = symbol.upper()
    print(f"Forward & Peers ETL called for symbol: {symbol}, peers: {peers}")
    
    # If no peers provided, fetch from API
    if peers is None or len(peers) == 0:
        peers = await get_peers(symbol)
    
    # If still no peers, use mock data
    if not peers or len(peers) == 0:
        print(f"Warning: No peers found for {symbol}, using mock data")
        peers = ["PEER1", "PEER2"]
    
    # Fetch earnings estimates
    estimates = await get_earnings_estimates(symbol)
    
    # Fetch price targets
    price_targets = await get_price_targets(symbol)
    
    # Process estimates
    est_df = await process_estimates(estimates)
    
    # If no estimates, use mock data
    if est_df.empty:
        print(f"Warning: No earnings estimates for {symbol}, using mock data")
        current_year = datetime.now().year
        est_df = pd.DataFrame({
            "year": [current_year, current_year + 1],
            "revenue": [110.0, 120.0],
            "netIncome": [22.0, 24.0],
            "ebitda": [35.0, 38.0]
        })
    
    # Prepare dataframes for peer operating and trading metrics
    oper_data = []
    trading_data = []
    
    # Limit to max 5 peers for efficiency
    for peer in peers[:5]:
        metrics = await get_key_metrics(peer)
        
        if metrics and len(metrics) > 0:
            # Extract operating metrics
            oper_row = {
                "symbol": peer,
                "revenue": metrics[0].get("revenuePerShare", 0) * metrics[0].get("weightedAverageShsOut", 1000000),
                "margin": metrics[0].get("netProfitMargin", 0)
            }
            oper_data.append(oper_row)
            
            # Extract trading metrics
            trading_row = {
                "symbol": peer,
                "pe": metrics[0].get("peRatioTTM", 0),
                "pb": metrics[0].get("pbRatioTTM", 0)
            }
            trading_data.append(trading_row)
    
    # If no operating data, use mock data
    if not oper_data:
        print(f"Warning: No operating data for peers of {symbol}, using mock data")
        oper_data = [
            {"symbol": "PEER1", "revenue": 95.0, "margin": 0.2},
            {"symbol": "PEER2", "revenue": 105.0, "margin": 0.22}
        ]
    
    # If no trading data, use mock data
    if not trading_data:
        print(f"Warning: No trading data for peers of {symbol}, using mock data")
        trading_data = [
            {"symbol": "PEER1", "pe": 15.0, "pb": 2.0},
            {"symbol": "PEER2", "pe": 16.0, "pb": 2.2}
        ]
    
    # Create dataframes
    oper_df = pd.DataFrame(oper_data)
    trading_df = pd.DataFrame(trading_data)
    
    # Process price targets
    pt_data = []
    if price_targets and len(price_targets) > 0:
        for pt in price_targets:
            pt_row = {
                "broker": pt.get("analystCompany", "Unknown"),
                "analyst": pt.get("analystName", "Unknown"),
                "publishedDate": pd.to_datetime(pt.get("publishedDate", datetime.now().strftime("%Y-%m-%d"))),
                "priceTarget": pt.get("priceTarget", 0),
                "direction": pt.get("change", "same")
            }
            pt_data.append(pt_row)
    
    # If no price targets, use mock data
    if not pt_data:
        print(f"Warning: No price targets for {symbol}, using mock data")
        pt_data = [
            {
                "broker": "Goldman",
                "analyst": "John Doe",
                "publishedDate": pd.to_datetime((datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")),
                "priceTarget": 150.0,
                "direction": "up"
            },
            {
                "broker": "Morgan",
                "analyst": "Jane Smith",
                "publishedDate": pd.to_datetime((datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d")),
                "priceTarget": 160.0,
                "direction": "same"
            }
        ]
    
    pt_df = pd.DataFrame(pt_data)
    
    return est_df, oper_df, trading_df, pt_df 