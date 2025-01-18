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

amcData_5min = tf.convert(amcData, (1, 5))
amcData_15min = tf.convert(amcData, (1, 15))
amcData_30min = tf.convert(amcData, (1, 30))


print("\n=====\namcData\n")
print(amcData)
print("\n=====\namcData_5min\n")
print(amcData_5min)
print("\n=====\namcData_15min\n")
print(amcData_15min)
print("\n=====\namcData_30min\n")
print(amcData_30min)