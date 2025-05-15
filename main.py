#!/usr/bin/env python3
"""
Main entry point for FMP ETL system.
This script provides functionality to:
1. Initialize the SQLite database
2. Run ETL process for a specific ticker
3. Export data for analysis
"""
import os
import sys
import asyncio
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession

from models import metadata
from combined_etl import combined_etl
from export_ticker_data import export_ticker_data

DEFAULT_DB_PATH = "./financial_data.sqlite"
DEFAULT_FMP_KEY = "fjRDKKnsRnVNMfFepDM6ox31u9RlPklv"  # Demo key, consider setting via env var

def init_db(db_path=DEFAULT_DB_PATH):
    """Initialize the SQLite database with the necessary schema
    
    Args:
        db_path (str): Path to SQLite database file
    
    Returns:
        bool: True if successful, False otherwise
    """
    # Convert async URI to sync URI for schema creation
    sync_uri = db_path.replace("sqlite+aiosqlite://", "sqlite://")
    
    try:
        # Create engine for synchronous connection
        engine = create_engine(sync_uri)
        
        # Create tables
        metadata.create_all(engine)
        
        print(f"Successfully initialized database at {db_path}")
        return True
    except Exception as e:
        print(f"Error initializing database: {e}")
        return False

async def run_etl(ticker, fmp_key=None, db_path=None, peers=None):
    """Run ETL process for a specific ticker
    
    Args:
        ticker (str): Ticker symbol to process
        fmp_key (str, optional): FMP API key. Defaults to environment variable or demo key.
        db_path (str, optional): Path to SQLite database file. Defaults to environment variable or default path.
        peers (list, optional): List of peer tickers. Defaults to empty list.
    
    Returns:
        bool: True if successful, False otherwise
    """
    ticker = ticker.upper()
    
    # Set up environment variables
    if fmp_key:
        os.environ["FMP_KEY"] = fmp_key
    elif "FMP_KEY" not in os.environ:
        os.environ["FMP_KEY"] = DEFAULT_FMP_KEY
        
    if db_path:
        db_uri = f"sqlite+aiosqlite:///{db_path}"
        os.environ["DATABASE_URL"] = db_uri
    elif "DATABASE_URL" not in os.environ:
        db_uri = f"sqlite+aiosqlite:///{DEFAULT_DB_PATH}"
        os.environ["DATABASE_URL"] = db_uri
    
    # Get the actual DB path for export function
    actual_db_path = db_path or DEFAULT_DB_PATH
    
    # Set default peers list
    if peers is None:
        peers = []
    
    try:
        print(f"Starting ETL process for {ticker}...")
        await combined_etl(ticker, peers)
        print(f"ETL process completed successfully for {ticker}")
        return True
    except Exception as e:
        print(f"Error during ETL process: {e}")
        return False

async def main_async():
    """Main async function"""
    if len(sys.argv) < 2:
        print("Usage: python main.py <command> [options]")
        print("Commands:")
        print("  init                   - Initialize database")
        print("  etl <ticker> [peers]   - Run ETL process for ticker")
        print("  export <ticker>        - Export data for ticker")
        print("  run <ticker> [peers]   - Initialize, ETL, and export in one step")
        return 1
    
    command = sys.argv[1].lower()
    
    # Set default database path
    db_path = os.getenv("DB_PATH", DEFAULT_DB_PATH)
    
    if command == "init":
        # Initialize database
        success = init_db(db_path)
        return 0 if success else 1
        
    elif command == "etl":
        # Run ETL process
        if len(sys.argv) < 3:
            print("Error: Ticker symbol required for ETL command")
            return 1
            
        ticker = sys.argv[2]
        peers = sys.argv[3:] if len(sys.argv) > 3 else []
        
        success = await run_etl(ticker, db_path=db_path, peers=peers)
        return 0 if success else 1
        
    elif command == "export":
        # Export data
        if len(sys.argv) < 3:
            print("Error: Ticker symbol required for export command")
            return 1
            
        ticker = sys.argv[2]
        success = export_ticker_data(ticker, db_path)
        return 0 if success else 1
        
    elif command == "run":
        # Run everything in one step
        if len(sys.argv) < 3:
            print("Error: Ticker symbol required for run command")
            return 1
            
        ticker = sys.argv[2]
        peers = sys.argv[3:] if len(sys.argv) > 3 else []
        
        # Initialize database
        init_success = init_db(db_path)
        if not init_success:
            return 1
            
        # Run ETL
        etl_success = await run_etl(ticker, db_path=db_path, peers=peers)
        if not etl_success:
            return 1
            
        # Export data
        export_success = export_ticker_data(ticker, db_path)
        return 0 if export_success else 1
        
    else:
        print(f"Unknown command: {command}")
        return 1

def main():
    """Main entry point"""
    return asyncio.run(main_async())

if __name__ == "__main__":
    sys.exit(main()) 