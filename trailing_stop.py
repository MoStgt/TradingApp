import config, tulipy
import alpaca_trade_api as tradeapi
from helpers import calculate_quantity
from datetime import date
import pandas as pd
from timezone import is_dst

# connection to alpaca api
api = tradeapi.REST(config.API_KEY, config.SECRET_KEY, base_url=config.API_URL)

# symbols = ['SPY', 'IWM', 'DIA']
#
# for symbol in symbols:
#
#     quote = api.get_last_quote(symbol)
#
#     api.submit_order(
#         symbol=symbol,
#         side='buy',
#         type='market',
#         qty=calculate_quantity(quote.bidprice),
#         time_in_force='day'
#     )

# orders = api.list_orders()
# positions = api.list_positions()

# api.submit_order(
#     symbol='IWM',
#     side='sell',
#     qty=57,
#     time_in_force='day',
#     type='trailing_stop',
#     trail_price='0.20'
# )
#
# api.submit_order(
#     symbol='DIA',
#     side='sell',
#     qty=5,
#     time_in_force='day',
#     type='trailing_stop',
#     trail_percent='0.70'
# )


start_bar = "2020-10-01T09:30:00-05:00"
end_bar = "2020-11-13T10:00:00-05:00"

# print(start_bar)
# print(type(start_bar))
#
# current_date = date.today().isoformat()
# NY = 'America/New_York'
# end = pd.Timestamp(f"{current_date} 16:00", tz=NY).isoformat()
# print(end)
# print(type(end))

# get data
daily_bars = api.get_barset('NIO', 'day', start=start_bar, end=end_bar).df

atr = tulipy.atr(daily_bars['NIO']['high'].values, daily_bars['NIO']['low'].values, daily_bars['NIO']['close'].values, 14)
print(atr)