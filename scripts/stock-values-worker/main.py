import yfinance as yf
from pymongo import MongoClient
import os

# MongoDb
mongo_uri = os.getenv('MONGO_URI', 'mongodb://192.168.3.201:27017/')
table = os.getenv('TABLE', 'nasdaq_index')
collection = os.getenv('collection', 'ixic')

# Finances
ticker = os.getenv('TICKER', '^IXIC')
period = os.getenv('PERIOD', '1d')

if __name__ == '__main__':
  yf = yf.Ticker(ticker)
  hist = yf.history(period)
  hist = hist.reset_index()

  client = MongoClient(mongo_uri)
  db = client[table]
  collection = db[collection]

  data_dict = hist.to_dict('records')
  collection.insert_many(data_dict)