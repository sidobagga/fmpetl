# Financial Modeling Prep ETL System

This repository contains a data pipeline for fetching financial data from the Financial Modeling Prep (FMP) API, processing it through an ETL pipeline, and storing it in a SQLite database for analysis.

## Features

- Fetch financial data from FMP API for any ticker symbol
- Process and transform data through an ETL pipeline
- Store data in a SQLite database
- Export data to CSV files for analysis
- API endpoints to query the data

## Requirements

- Python 3.8+
- Required packages:
  - sqlalchemy
  - aiosqlite
  - asyncio
  - pandas
  - fastapi (for API)
  - uvicorn (for API)

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/sidobagga/fmpetl.git
   cd fmpetl
   ```

2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

## Configuration

The system uses environment variables for configuration:

- `FMP_KEY` - Your Financial Modeling Prep API key (defaults to a demo key)
- `DATABASE_URL` - SQLite database URL (defaults to `sqlite+aiosqlite:///financial_data.sqlite`)
- `DB_PATH` - Path to the SQLite database file (defaults to `./financial_data.sqlite`)

## Usage

### Initialize Database

To create the SQLite database and tables:

```
python main.py init
```

### Run ETL Process

To run the ETL process for a specific ticker:

```
python main.py etl MSFT
```

To include peer companies:

```
python main.py etl MSFT AAPL GOOG
```

### Export Data

To export data for a specific ticker to CSV:

```
python main.py export MSFT
```

This will create three CSV files:
- `MSFT_quarterly.csv` - Quarterly financial data for Microsoft
- `MSFT_competitors.csv` - Competitor data for Microsoft
- `MSFT_and_competitors.csv` - Combined data for Microsoft and its competitors

### Run Complete Process

To initialize the database, run ETL, and export data in one step:

```
python main.py run MSFT
```

### Using the API

To start the API server:

```
uvicorn api:app --reload
```

API endpoints:
- `GET /metrics/{symbol}?metrics=revenue&metrics=netIncome&period_type=annual` - Get financial metrics for a symbol
- `GET /peers/{symbol}/operating` - Get operating peers for a symbol
- `GET /peers/{symbol}/trading` - Get trading peers for a symbol

## File Structure

- `main.py` - Main entry point
- `models.py` - Database models
- `combined_etl.py` - Combined ETL process
- `fmp_etl.py` - Core ETL functions
- `fmp_forward_and_comp_peers.py` - Forward-looking estimates and competitor data
- `export_ticker_data.py` - Export data to CSV
- `api.py` - FastAPI endpoints

## Example

Fetch data for Microsoft, run ETL, and export to CSV:

```
python main.py run MSFT
```

Access the data via API:

```
curl http://localhost:8000/metrics/MSFT?metrics=revenue&metrics=netIncome
```

## License

MIT # Successfully tested with AAPL
