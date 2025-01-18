from libs.StockData import *
from libs.TimeframeData import *

print("hello")

stock_data = StockData()

# Example list of symbols
symbols = ['AAPL', 'TSLA', 'GOOGL', 'AMC', 'GME']

# Get data (will use cache if available)
data = stock_data.get_data(symbols, interval='1m', period='5d')

amcData = stock_data.symbolData(data, 'AMC')

tf = TimeframeData()
data_30min = tf.convertMany(data, (1, 30)) # convert from 1min to 30min

amcData_30min = tf.convert(amcData, (1, 15)) # convert from 1min to 15min
#amcData_1d = tf.convert(amcData, (1, '1d')) # convert from 1min to daily data
#amcData_3d = tf.convert(amcData, (1, '3d')) # convert from 1min to 3-day data


print("\n=====\nData\n")
print(data)
print("\n=====\ndata_30min\n")
print(data_30min)
print("\n=====\namcData\n")
print(amcData)
print("\n=====\namcData_30min\n")
print(amcData_30min)