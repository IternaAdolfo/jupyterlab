import backtrader as bt
import yfinance as yf
import pandas as pd
import requests_cache
from tabulate import tabulate  # Import tabulate for table formatting

# Set up session caching for yfinance
yf.set_tz_cache_location("custom/cache/location")
session = requests_cache.CachedSession('yfinance.cache')
session.headers['User-agent'] = 'my-program/1.0'

class TestStrategy(bt.Strategy):
    params = (
        ('MAX', None),  # Add MAX as a parameter
    )

    def log(self, txt, dt=None):
        """ Logging function for this strategy """
        dt = dt or self.datas[0].datetime.date(0)
    
    def __init__(self):
        # Initialize the EMAs for high and low prices
        self.ema_high = bt.indicators.ExponentialMovingAverage(self.datas[0].high, period=89)
        self.ema_low = bt.indicators.ExponentialMovingAverage(self.datas[0].low, period=89)
        self.dataclose = self.datas[0].close
        self.datalow = self.datas[0].low
        self.datahigh = self.datas[0].high
        self.open_trade_profit = 0  # Track open trade profit
        self.trade_count = 0  # Counter for number of trades
        self.open_positions = []  # Track open positions
        
        # Use the MAX parameter passed during initialization
        self.MAX = self.params.MAX
        
        # For MDD calculation
        self.equity_curve = []  # To store portfolio value at each step
    
    def stop(self):
        # Calculate MDD after the backtest ends
        equity_curve = pd.Series(self.equity_curve)
        cumulative_returns = equity_curve / equity_curve.iloc[0]
        peak = cumulative_returns.cummax()
        drawdown = (cumulative_returns - peak) / peak
        self.max_drawdown = drawdown.min() * 100  # Maximum Drawdown in percentage    
    
    def next(self):
        # Calculate portfolio value (cash + unrealized PnL)
        portfolio_value = self.broker.getvalue()        
        
        # Append portfolio value to equity curve
        self.equity_curve.append(portfolio_value)
        
        # Buy if close above EMA 89 of high
        if self.datalow[0] > self.ema_high[0]:
            if not self.position:
                cash = self.broker.getcash()
                size = float(cash / self.datalow[0] / self.MAX)
                self.log(f'BUY CREATE, Price: {self.datalow[0]:.2f}, Size: {size}')
                self.buy(size=size)
                self.open_positions.append(self.datalow[0])
                self.trade_count += 1
            elif self.trade_count < self.MAX:
                # Check if the price is higher than the last entry
                if self.datalow[0] > self.open_positions[-1]:
                    cash = self.broker.getcash()
                    size = float(cash / self.dataclose[0] / self.MAX)
                    self.log(f'BUY ADDITIONAL POSITION, Price: {self.dataclose[0]:.2f}, Size: {size}')
                    self.buy(size=size)
                    self.open_positions.append(self.dataclose[0])
                    self.trade_count += 1
        
        # Sell if close below EMA 89 of low
        elif self.datahigh[0] < self.ema_low[0] and self.datalow[0] <= self.position.price:
            if self.position:
                self.log(f'SELL CREATE, Price: {self.datahigh[0]:.2f}')
                self.sell(size=self.position.size)
                self.open_positions.pop(0)
                self.trade_count += 1

def resample_data(data_df, timeframe):
    ohlc_dict = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }
    return data_df.resample(timeframe).apply(ohlc_dict).dropna()

# Function to run backtest for a given MAX value
def run_backtest(max_trades):
    # Initialize Cerebro
    cerebro = bt.Cerebro()
    
    # List of symbols and timeframes
    symbols = ['BTC-USD', 'ETH-USD', 'LINK-USD']
    timeframes = {'1D': '1d', '4H': '4h', '8H': '8h'}
    
    # List to store results
    results_table = []
    
    # Loop through each symbol and timeframe
    for symbol in symbols:
        for tf_name, tf_interval in timeframes.items():
            # Download hourly data using yfinance
            data_df = yf.download(symbol, start='2024-01-01', end='2025-01-01', interval='1h', progress=False)
            
            # Handle Multi-Level Index
            if isinstance(data_df.columns, pd.MultiIndex):
                data_df.columns = data_df.columns.get_level_values(0)
            
            # Convert column names to lowercase for Backtrader
            data_df.columns = [col.lower() for col in data_df.columns]
            
            # Resample data for 4H and 8H timeframes
            if tf_interval in ['4h', '8h']:
                resampled_tf = '4h' if tf_interval == '4h' else '8h'
                data_df = resample_data(data_df, resampled_tf)
            
            # Create Backtrader Data Feed
            data = bt.feeds.PandasData(dataname=data_df)
            
            # Add data to Cerebro
            cerebro.adddata(data)

            cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
            
            # Set initial cash
            initial_value = 100000.0
            cerebro.broker.setcash(initial_value)

            # Set commission to 0.1%
            cerebro.broker.setcommission(commission=0.001)
            
            # Add a strategy with the specified MAX value
            cerebro.addstrategy(TestStrategy, MAX=max_trades)
            
            # Run the backtest
            results = cerebro.run() 
            
            # Access the strategy instance to get open trade profit and trade count
            strategy = results[0]
            trade_count = getattr(strategy, 'trade_count', 0)
            max_drawdown = getattr(strategy, 'max_drawdown', 0)
            drawdown_analysis = results[0].analyzers.drawdown.get_analysis()

            
            # Calculate performance metrics
            final_value = cerebro.broker.getvalue()        
            realized_profit_percent = ((final_value - initial_value) / initial_value) * 100
            
            current_price = data_df['close'].iloc[-1]
            unrealized_profit = sum((current_price - entry_price) / entry_price * 100 for entry_price in strategy.open_positions)
            unrealized_profit_percent = unrealized_profit       
           
            # Append results to the table
            results_table.append({
                "Asset": symbol,
                "Timeframe": tf_name,
                "Realized Profit (%)": f"{realized_profit_percent:.2f}",
                "Unrealized Profit (%)": f"{unrealized_profit_percent:.2f}",
                "Trades": trade_count,
                "Max Drawdown (%)": f"{max_drawdown:.2f}",
                "DrawdownAnalysis (%)": f"{drawdown_analysis['max']['drawdown']:.2f}", 
            })
            
            # Reset Cerebro for the next iteration
            cerebro = bt.Cerebro()
    
    return results_table

# Run backtests for MAX = 5 and MAX = 3
results_max_5 = run_backtest(max_trades=5)
results_max_3 = run_backtest(max_trades=3)

results_max_8 = run_backtest(max_trades=8)
results_max_13 = run_backtest(max_trades=13)

# Print results for MAX = 5
print("\nResults for MAX = 5:")
print(tabulate(results_max_5, headers="keys", tablefmt="grid"))

# Print results for MAX = 3
print("\nResults for MAX = 3:")
print(tabulate(results_max_3, headers="keys", tablefmt="grid"))

# Print results for MAX = 8
print("\nResults for MAX = 8:")
print(tabulate(results_max_8, headers="keys", tablefmt="grid"))

# Print results for MAX = 13
print("\nResults for MAX = 13:")
print(tabulate(results_max_13, headers="keys", tablefmt="grid"))