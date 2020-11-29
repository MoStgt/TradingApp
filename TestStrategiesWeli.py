import pandas as pd
import sqlite3
import config
import matplotlib.pyplot as plt
import datetime

# Connect to created db file
connection = sqlite3.connect(config.DB_FILE)

df=pd.DataFrame()
df = pd.read_sql_query(" SELECT * FROM stock_price_minute WHERE stock_id = '8868' ", connection)


df['time'] = [d[11:] for d in df['datetime']]
df = df.loc[df.time.between('09:30:00', '16:00:00')]


df['Points Change'] = df['close'].diff()

print("Max. increase in 1 minute ", df['Points Change'].loc[df['Points Change'].idxmax()], ' on ', df['datetime'].loc[df['Points Change'].idxmax()])
print("Min. decrease in 1 minute ", df['Points Change'].loc[df['Points Change'].idxmin()], ' on ', df['datetime'].loc[df['Points Change'].idxmin()])

df.set_index('datetime')
print(len(df['datetime']))
print(len(df['close']))
