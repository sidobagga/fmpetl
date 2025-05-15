#!/usr/bin/env python3
"""
Export financial data for any ticker and its competitors to CSV
"""
import sqlite3
import pandas as pd
import json
import sys
import os

def export_ticker_data(ticker, db_path="./financial_data.sqlite"):
    """Export ticker data and its competitors to CSV files
    
    Args:
        ticker (str): Ticker symbol to export data for
        db_path (str): Path to SQLite database file
    
    Returns:
        bool: True if successful, False otherwise
    """
    if not os.path.exists(db_path):
        print(f"Error: Database file {db_path} does not exist!")
        return False
    
    ticker = ticker.upper()
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Check if ticker exists in the database
    cursor.execute("SELECT id FROM symbols WHERE ticker = ?", (ticker,))
    symbol_row = cursor.fetchone()
    
    if not symbol_row:
        print(f"Error: Ticker {ticker} not found in database. Run ETL process first.")
        conn.close()
        return False
    
    symbol_id = symbol_row[0]
    
    # Get quarterly financial data
    cursor.execute("""
        SELECT p.period_end, m.metric_code, m.value
        FROM financial_metrics m
        JOIN financial_periods p ON p.id = m.period_id
        WHERE p.symbol_id = ? AND p.period_type = 'quarter'
        ORDER BY p.period_end DESC
    """, (symbol_id,))
    
    quarterly_data = cursor.fetchall()
    
    # Organize data by quarter
    quarters = {}
    for period_end, metric, value in quarterly_data:
        if period_end not in quarters:
            quarters[period_end] = {'period_end': period_end}
        quarters[period_end][metric] = value
    
    # Convert to DataFrame
    df = pd.DataFrame(list(quarters.values()))
    
    # Export quarterly data to CSV
    quarterly_csv_file = f"{ticker}_quarterly.csv"
    if not df.empty:
        df.to_csv(quarterly_csv_file, index=False)
        print(f"Exported {ticker} quarterly data to {quarterly_csv_file}")
        print(f"\n{ticker} Quarterly Data:")
        print(df.head())
    else:
        print(f"No quarterly data found for {ticker}")
    
    # Get operating peers data
    cursor.execute("""
        SELECT peer_ticker, payload
        FROM peer_operating
        WHERE host_symbol_id = ?
    """, (symbol_id,))
    operating_peers = cursor.fetchall()
    
    # Get trading peers data
    cursor.execute("""
        SELECT peer_ticker, payload
        FROM peer_trading
        WHERE host_symbol_id = ?
    """, (symbol_id,))
    trading_peers = cursor.fetchall()
    
    # Combine data
    peer_data = []
    
    # Process operating peers
    for peer, payload_str in operating_peers:
        data = {"ticker": peer, "data_type": "operating"}
        
        # Parse the JSON payload
        try:
            payload = json.loads(payload_str)
            # Add each metric from the payload
            for key, value in payload.items():
                data[f"operating_{key}"] = value
        except json.JSONDecodeError:
            # Handle case where payload is already a dict
            payload = eval(payload_str)
            for key, value in payload.items():
                data[f"operating_{key}"] = value
        except Exception as e:
            print(f"Warning: Could not parse operating payload for {peer}: {e}")
            continue
        
        peer_data.append(data)
    
    # Process trading peers
    for peer, payload_str in trading_peers:
        # Find if this peer is already in the list
        found = False
        for item in peer_data:
            if item["ticker"] == peer:
                found = True
                item["data_type"] = "both"
                
                # Parse the JSON payload
                try:
                    payload = json.loads(payload_str)
                    # Add each metric from the payload
                    for key, value in payload.items():
                        item[f"trading_{key}"] = value
                except json.JSONDecodeError:
                    # Handle case where payload is already a dict
                    payload = eval(payload_str)
                    for key, value in payload.items():
                        item[f"trading_{key}"] = value
                except Exception as e:
                    print(f"Warning: Could not parse trading payload for {peer}: {e}")
                    continue
                
                break
        
        # If not found, add as new entry
        if not found:
            data = {"ticker": peer, "data_type": "trading"}
            
            # Parse the JSON payload
            try:
                payload = json.loads(payload_str)
                # Add each metric from the payload
                for key, value in payload.items():
                    data[f"trading_{key}"] = value
            except json.JSONDecodeError:
                # Handle case where payload is already a dict
                payload = eval(payload_str)
                for key, value in payload.items():
                    data[f"trading_{key}"] = value
            except Exception as e:
                print(f"Warning: Could not parse trading payload for {peer}: {e}")
                continue
            
            peer_data.append(data)
    
    # Get quarterly financial data for these peers
    for peer_data_item in peer_data:
        peer_ticker = peer_data_item["ticker"]
        
        # Check if this ticker exists in the symbols table
        cursor.execute("SELECT id FROM symbols WHERE ticker = ?", (peer_ticker,))
        peer_symbol_row = cursor.fetchone()
        
        if peer_symbol_row:
            peer_symbol_id = peer_symbol_row[0]
            
            # Get quarterly financial data
            cursor.execute("""
                SELECT p.period_end, m.metric_code, m.value
                FROM financial_metrics m
                JOIN financial_periods p ON p.id = m.period_id
                WHERE p.symbol_id = ? AND p.period_type = 'quarter'
                ORDER BY p.period_end DESC
            """, (peer_symbol_id,))
            
            financial_data = cursor.fetchall()
            
            if financial_data:
                # Group by period
                by_period = {}
                for period_end, metric, value in financial_data:
                    by_period.setdefault(period_end, {})
                    by_period[period_end][metric] = value
                
                # Add the most recent period's data
                if by_period:
                    most_recent = sorted(by_period.keys(), reverse=True)[0]
                    for metric, value in by_period[most_recent].items():
                        peer_data_item[f"quarterly_{metric}"] = value
                    peer_data_item["quarter_end"] = most_recent
    
    # Create DataFrame for peers
    peers_df = pd.DataFrame(peer_data)
    
    # Export peers data to CSV
    if not peers_df.empty:
        peers_csv_file = f"{ticker}_competitors.csv"
        peers_df.to_csv(peers_csv_file, index=False)
        print(f"Exported {ticker} competitor data to {peers_csv_file}")
        
        # Show sample of the data
        print(f"\nCompetitor Sample data for {ticker}:")
        print(peers_df.head())
        
        # Create a combined CSV with ticker data and its competitors
        # Add ticker's own data as a row
        ticker_row = {
            "ticker": ticker,
            "data_type": "self"
        }
        
        # Add the most recent quarterly data for the ticker
        if quarters:
            most_recent = sorted(quarters.keys(), reverse=True)[0]
            ticker_data = quarters[most_recent]
            for metric, value in ticker_data.items():
                if metric != 'period_end':
                    ticker_row[f"quarterly_{metric}"] = value
            ticker_row["quarter_end"] = most_recent
        
        # Add ticker's row to peers data
        combined_df = pd.concat([pd.DataFrame([ticker_row]), peers_df], ignore_index=True)
        
        # Export combined data
        combined_csv_file = f"{ticker}_and_competitors.csv"
        combined_df.to_csv(combined_csv_file, index=False)
        print(f"Exported combined {ticker} and competitor data to {combined_csv_file}")
        
        # Show combined data
        print(f"\nCombined data sample for {ticker}:")
        print(combined_df.head())
    else:
        print(f"No competitor data found for {ticker}")
    
    conn.close()
    return True

def main():
    if len(sys.argv) < 2:
        print("Usage: python export_ticker_data.py TICKER [DB_PATH]")
        return 1
    
    ticker = sys.argv[1]
    db_path = sys.argv[2] if len(sys.argv) > 2 else "./financial_data.sqlite"
    
    success = export_ticker_data(ticker, db_path)
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main()) 