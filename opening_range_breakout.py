import sqlite3
import config
import alpaca_trade_api as tradeapi
from datetime import date
import pandas as pd
import smtplib, ssl
from timezone import is_dst

# Create a secure SSL context
context = ssl.create_default_context()

# Connect to created db file
connection = sqlite3.connect(config.DB_FILE)
connection.row_factory = sqlite3.Row

# get stocks symbols with certain strategy selected earlier from created db file
cursor = connection.cursor()

cursor.execute("""
    select id from strategy where name = 'opening_range_breakout'
    """)

strategy_id = cursor.fetchone()['id']

cursor.execute("""
    select symbol, name
    from stock
    join stock_strategy on stock_strategy.stock_id = stock.id
    where stock_strategy.strategy_id = ?
    """, (strategy_id,))

stocks = cursor.fetchall()
symbols = [stock['symbol'] for stock in stocks]

# set current date
current_date = date.today().isoformat()
# current_date = "2020-11-17"

# define start and end minute for breakout strategy
if is_dst():
    start_minute_bar = f"{current_date}T09:30:00-5:00"
    end_minute_bar = f"{current_date}T10:00:00-5:00"
else:
    start_minute_bar = f"{current_date}T09:30:00-4:00"
    end_minute_bar = f"{current_date}T10:00:00-4:00"

# define timeframe of stocks
NY = 'America/New_York'
start = pd.Timestamp(f"{current_date} 9:30", tz=NY).isoformat()
end = pd.Timestamp(f"{current_date} 16:00", tz=NY).isoformat()

# connection to alpaca api
api = tradeapi.REST(config.API_KEY, config.SECRET_KEY, base_url=config.API_URL)

# check already existing orders (UTC time -> 9.30am New York time)
orders = api.list_orders(status='all', after=current_date)
existing_order_symbols = [order.symbol for order in orders if order.status != 'canceled']

# Messages to send as an Email
messages = []

# loop through symbols of investigating stocks
for symbol in symbols:

    # get data
    minute_bars = api.get_barset(symbol, 'minute', start=start, end=end).df

    # get data for defined breakout timeframe
    opening_range_mask = (minute_bars.index >= start_minute_bar) & (minute_bars.index < end_minute_bar)
    opening_range_bars = minute_bars.loc[opening_range_mask]

    # caculate range of breakout
    opening_range_low = opening_range_bars[symbol]['low'].min()
    opening_range_high = opening_range_bars[symbol]['high'].max()
    opening_range = opening_range_high - opening_range_low

    # defining timeframe and get data after breakout timeframe
    after_opening_range_mask = (minute_bars.index >= end_minute_bar)
    after_opening_range_bars = minute_bars.loc[after_opening_range_mask]

    # check if condition is met
    after_opening_range_breakout = after_opening_range_bars[after_opening_range_bars[symbol]['close'] > opening_range_high]

    if not after_opening_range_breakout.empty:
        if symbol not in existing_order_symbols:

            limit_price = after_opening_range_breakout[symbol].iloc[0]['close']

            messages.append(f"Placing order for {symbol} at {limit_price}, closed above {opening_range_high}\n\n{after_opening_range_breakout.iloc[0]}\n\n")

            #print(f"Placing order for {symbol} at {limit_price}, closed above {opening_range_high} at {after_opening_range_breakout.iloc[0]}")

            api.submit_order(
                symbol=symbol,
                side='buy',
                type='limit',
                qty='10',
                time_in_force='day',
                order_class='bracket',
                limit_price=limit_price,
                take_profit=dict(
                    limit_price=limit_price+opening_range,
                ),
                stop_loss=dict(
                    stop_price=limit_price-opening_range,
                )
            )
        else:
            print(f"Already an order for {symbol}, skipping")

print(messages)
# connection to Email server
with smtplib.SMTP_SSL(config.EMAIL_HOST, config.EMAIL_PORT, context=context) as server:
    server.login(config.EMAIL_ADDRESS, config.EMAIL_PASSWORD)

    # email subject
    email_message = f"Subject: Trade Notifications for {current_date}\n\n"

    # convert messages to string messages
    email_message += "\n\n".join(messages)

    # send email
    server.sendmail(config.EMAIL_ADDRESS, config.EMAIL_ADDRESS, email_message)