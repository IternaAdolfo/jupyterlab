import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from tabulate import tabulate
import os
import pickle

# Cache file name
CACHE_FILE = "crypto_data_cache.pkl"

# Step 1: Fetch top crypto assets by market cap (weekly performance)
def get_top_crypto_assets():
    # Define a list of popular crypto tickers
    crypto_tickers = [
        "BTC-USD", "ETH-USD", "BNB-USD", "XRP-USD", "ADA-USD",
        "SOL-USD", "DOT-USD", "DOGE-USD", "MATIC-USD", "LTC-USD",
        "TRX-USD", "AVAX-USD", "LINK-USD", "UNI-USD", "ATOM-USD",
        "ALGO-USD", "FTT-USD", "VET-USD", "FIL-USD", "AAVE-USD"
    ]
    
    # Check if cached data exists and is recent
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "rb") as f:
                cached_data = pickle.load(f)
            
            # Check if the cached data is less than 1 day old
            if datetime.now() - cached_data["timestamp"] < timedelta(days=1):
                print("Using cached data...")
                return cached_data["data"]
        except Exception as e:
            print(f"Failed to load cache: {e}")
            cached_data = None
    
    # Fetch fresh data if no valid cache exists
    print("Fetching fresh data from yfinance...")
    end_date = datetime.now()
    start_date = end_date - timedelta(days=4 * 365)  # Last 4 years
    
    crypto_data = {}
    for ticker in crypto_tickers:
        data = yf.download(ticker, start=start_date, end=end_date, interval="1wk")
        if not data.empty:
            # Ensure the data has at least 4 years of weekly data
            if len(data) >= 200:  # Roughly 4 years of weekly data (52 weeks/year * 4 years)
                crypto_data[ticker] = data
            else:
                print(f"Skipping {ticker} due to insufficient weekly data.")
        else:
            print(f"No data available for {ticker}.")
    
    # Save fetched data to cache with explicit protocol version
    try:
        with open(CACHE_FILE, "wb") as f:
            pickle.dump({"timestamp": datetime.now(), "data": crypto_data}, f, protocol=4)  # Use protocol 4
    except Exception as e:
        print(f"Failed to save cache: {e}")
    
    return crypto_data

# Step 2: Calculate weekly, quarterly, and yearly performance
def calculate_performance(crypto_data):
    all_weekly_performance = []
    all_quarterly_performance = []
    all_yearly_performance = []
    
    for ticker, data in crypto_data.items():
        # Handle MultiIndex columns if present
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.droplevel(1)  # Flatten the MultiIndex            
        
        # Ensure the "Close" column exists
        if "Close" not in data.columns:
            print(f"Skipping {ticker} due to missing 'Close' column.")
            continue
        
        # Calculate weekly performance
        weekly_performance = data["Close"].pct_change() * 100  # Percentage change between weeks
        weekly_performance = weekly_performance.dropna()  # Drop NaN values (first week has no prior week)
        
        # Store weekly performance
        for idx, performance in enumerate(weekly_performance, start=1):
            all_weekly_performance.append({
                "Asset": ticker,
                "Week": f"Week_{idx}",  # Use proper naming convention
                "Weekly Performance (%)": performance
            })
        
        # Group data into quarters (every 3 weeks) and calculate quarterly performance
        quarterly_data = data["Close"].resample("3W").last()  # Resample to 3-week intervals
        quarterly_performance = quarterly_data.pct_change() * 100  # Percentage change between quarters
        quarterly_performance = quarterly_performance.dropna()
        
        # Store quarterly performance
        for q_idx, performance in enumerate(quarterly_performance, start=1):
            all_quarterly_performance.append({
                "Asset": ticker,
                "Quarter": f"Quarter_{q_idx}",  # Use proper naming convention
                "Quarterly Performance (%)": performance
            })
        
        # Group data into years (every 52 weeks) and calculate yearly performance
        yearly_data = data["Close"].resample("52W").last()  # Resample to 52-week intervals
        yearly_performance = yearly_data.pct_change() * 100  # Percentage change between years
        yearly_performance = yearly_performance.dropna()
        
        # Store yearly performance
        for y_idx, performance in enumerate(yearly_performance, start=1):
            all_yearly_performance.append({
                "Asset": ticker,
                "Year": f"Year_{y_idx}",  # Use proper naming convention
                "Yearly Performance (%)": performance
            })
    
    # Create DataFrames for weekly, quarterly, and yearly performance
    weekly_df = pd.DataFrame(all_weekly_performance)
    quarterly_df = pd.DataFrame(all_quarterly_performance)
    yearly_df = pd.DataFrame(all_yearly_performance)
    
    # Pivot the DataFrames to wide format
    wide_weekly_df = weekly_df.pivot(index="Asset", columns="Week", values="Weekly Performance (%)")
    wide_quarterly_df = quarterly_df.pivot(index="Asset", columns="Quarter", values="Quarterly Performance (%)")
    wide_yearly_df = yearly_df.pivot(index="Asset", columns="Year", values="Yearly Performance (%)")
    
    # Combine all performance metrics into a single DataFrame
    combined_df = pd.concat([wide_weekly_df, wide_quarterly_df, wide_yearly_df], axis=1)
    
    return combined_df

# Step 3: Apply color coding to the table
def apply_color_coding(wide_df):
    def get_color(value):
        if pd.isnull(value):  # Handle missing values
            return "\033[38;2;255;255;255m"  # White for missing values
        if value > 0:
            # Green gradient (light to dark)
            intensity = 255-min(255, int(255 * (value / 100)))
            return f"\033[38;2;{intensity};255;{intensity}m"
        elif value < 0:
            # Red gradient (light to dark)
            intensity = 255-min(255, int(255 * (-value / 100)))
            return f"\033[38;2;255;{intensity};{intensity}m"
        else:
            # Neutral (white)
            return "\033[38;2;255;255;255m"
    
    # Apply color coding to each cell
    colored_rows = []
    for asset in wide_df.index:
        row = [asset]  # Start with the asset name
        for val in wide_df.loc[asset]:
            color = get_color(val)
            reset_color = "\033[0m"  # Reset color after each cell
            if pd.notnull(val):
                row.append(f"{color}{val:.2f}%{reset_color}")
            else:
                row.append("")  # Empty string for missing values
        colored_rows.append(row)
    
    return colored_rows

# Step 4: Display the table
def display_table(colored_rows, wide_df):
    headers = ["Asset"] + list(wide_df.columns)
    print(tabulate(colored_rows, headers=headers, tablefmt="grid"))

# Main execution
crypto_data = get_top_crypto_assets()
wide_df = calculate_performance(crypto_data)
colored_rows = apply_color_coding(wide_df)
display_table(colored_rows, wide_df)