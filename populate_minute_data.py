import config
import sqlite3
import pandas as pd
import csv
import alpaca_trade_api as tradeapi
from datetime import datetime, timedelta



connection = sqlite3.connect(config.DB_FILE)
connection.row_factory = sqlite3.Row
cursor = connection.cursor()


# compare selected stocks in stocks db and give selected stocks the right id
symbols=[]
stock_ids ={}

with open('qqqETF.csv') as f:
    reader = csv.reader(f)
    for line in reader:
        symbols.append(line[1])

cursor.execute("""
    SELECT * FROM stock
""")

stocks = cursor.fetchall()

for stock in stocks:
    symbol = stock['symbol']
    stock_ids[symbol] = stock['id']

for symbol in symbols:

    # get data
    start_date = datetime(2020, 1, 1).date()
    #start_date = f"{start_date}T00:00:00-05:00"
    end_date_range = datetime(2020, 11, 20).date()
    #end_date_range = f"{end_date_range}T23:59:59-05:00"

    while start_date < end_date_range:
        end_date = start_date + timedelta(days=4)

        print(f"=== Fetching minute bars {start_date} - {end_date} for {symbol}")
        api = tradeapi.REST(config.API_KEY, config.SECRET_KEY, config.API_URL)

        end_date_bars = f"{end_date}T23:59:59-05:00"
        start_date_bars = f"{start_date}T00:00:00-05:00"
        minutes = api.get_barset(symbol, 'minute', start=start_date_bars, end=end_date_bars).df
        print(minutes)
        # forward filling of missing data
        minutes = minutes.resample('1min').ffill()

        for index, row in minutes.iterrows():
           #going through minutes data and insert into db
            cursor.execute("""
                INSERT INTO stock_price_minute (stock_id, datetime, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (stock_ids[symbol], index.tz_localize(None).isoformat(), row[symbol]['open'], row[symbol]['high'], row[symbol]['low'], row[symbol]['close'], row[symbol]['volume']))

        start_date = start_date + timedelta(days=7)

connection.commit()