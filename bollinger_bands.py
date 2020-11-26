import sqlite3
import config, tulipy
import alpaca_trade_api as tradeapi
from datetime import date
import pandas as pd

# Connect to created db file
connection = sqlite3.connect(config.DB_FILE)
connection.row_factory = sqlite3.Row

# get stocks symbols with certain strategy selected earlier from created db file
cursor = connection.cursor()

cursor.execute("""
    select id from strategy where name = 'bollinger_bands'
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


# define timeframe of stocks
NY = 'America/New_York'
start = pd.Timestamp(f"{current_date} 9:30", tz=NY).isoformat()
end = pd.Timestamp(f"{current_date} 14:28", tz=NY).isoformat()

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
    # get data for defined timeframe
    market_open_mask = (minute_bars.index >= start) & (minute_bars.index < end)
    market_open_bars = minute_bars.loc[market_open_mask]

    # wait until market open for 20 min on current day
    if len(market_open_bars) >= 20:
        # get closing prices from bars
        closes = market_open_bars[symbol].close.values

        # calculate bollinger bands from tulipy package
        lower, middle, upper = tulipy.bbands(closes, 20, 2)


        # previous candle[-2] below bollinger band and latest candle[-1] above lower bollinger band, enter long position
        current_candle = market_open_bars[symbol].iloc[-1]
        previous_candle = market_open_bars[symbol].iloc[-2]

        if current_candle.close > lower[-1] and previous_candle.close < lower[-2]:
            print(f"{symbol} closed above lower bollinger band")


            if symbol not in existing_order_symbols:

                limit_price = current_candle.close

                candle_range = current_candle.high - current_candle.low
                print(f"placing order for {symbol} at {limit_price}")
                messages.append(
                    f"Placing order for {symbol} at {limit_price}")

                # print(f"Placing order for {symbol} at {limit_price}, closed above {opening_range_high} at {after_opening_range_breakout.iloc[0]}")

                api.submit_order(
                    symbol=symbol,
                    side='buy',
                    type='limit',
                    qty='10',
                    time_in_force='day',
                    order_class='bracket',
                    limit_price=limit_price,
                    take_profit=dict(
                        limit_price=limit_price + (candle_range*3),
                    ),
                    stop_loss=dict(
                        stop_price=previous_candle.low - candle_range,
                    )
                )
            else:
                print(f"Already an order for {symbol}, skipping")
