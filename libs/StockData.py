import os
import pickle
import yfinance as yf
import hashlib
from pathlib import Path

class StockData:
    def __init__(self, cache_dir='./cache'):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)  # Ensure the cache directory exists

    def _get_cache_file(self, symbols, interval, period):
        # Sort symbols, create a hash for consistent filenames
        symbols_str = ','.join(sorted(symbols))
        hash_input = f"{symbols_str}|{interval}|{period}"
        hash_digest = hashlib.sha256(hash_input.encode()).hexdigest()
        return self.cache_dir / f"stockdata_{hash_digest}.pkl"

    def get_data(self, symbols, interval='1d', period='1y'):
        cache_file = self._get_cache_file(symbols, interval, period)
        if cache_file.exists():
            with open(cache_file, 'rb') as f:
                data = pickle.load(f)
            return data
        else:
            return self.refresh_data(symbols, interval, period)

    def refresh_data(self, symbols, interval='1d', period='1y'):
        data = yf.download(symbols, interval=interval, period=period,
                           group_by='ticker', threads=True)
        cache_file = self._get_cache_file(symbols, interval, period)
        with open(cache_file, 'wb') as f:
            pickle.dump(data, f)
        return data

    def symbolData(self, data, symbol):
        if symbol not in data.columns.get_level_values(0):
            raise ValueError(f"Symbol {symbol} not found in the provided data.")
        return data[symbol]

# Usage example
if __name__ == "__main__":
    stock_data = StockData()

    # Example list of symbols
    symbols = ['AAPL', 'TSLA', 'GOOGL', 'AMC', 'GME']

    # Get data (will use cache if available)
    data = stock_data.get_data(symbols, interval='1m', period='5d')
    print(data)

    symbolData = stock_data.symbolData(data, 'AMC')
    print(symbolData)
