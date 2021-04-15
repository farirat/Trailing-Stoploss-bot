"""
This script will open a new position no matter the price.

It is based on the market_settings collection
"""
import yaml
import argparse
import time
import datetime as dt
from pymongo import MongoClient

from binance.client import Client as Binance
from binance.enums import *

parser = argparse.ArgumentParser(description='Scalper bot.')
parser.add_argument('--config', type=str, required=False, default="config.yml",
                    help='Config file')

args = parser.parse_args()
exchange = 'binance'

try:
    # Load configuration
    config = yaml.load(open(args.config, 'r'), Loader=yaml.SafeLoader)

    # Initialize mongo api
    mongo = MongoClient(config.get('db', None))
    mongo.server_info()
    db = mongo[config.get('db_name', 'dumbot')]

    # Exchange API keys
    API_KEY = config.get('%s_api_key' % exchange, None)
    API_SECRET = config.get('%s_api_secret' % exchange, None)
    SLEEP_SECONDS = 60

    while True:
        # Re-Initialize binance api
        api = Binance(API_KEY, API_SECRET)

        # Is binance alive ?
        if api.get_system_status().get("status", -1) != 0:
            raise Exception("Exchange unavailable for trading")

        # Scalping in progress:
        for market in db.scalping_settings.find({"scalping": True}):
            try:
                # Get ticker lastPrice and asset balance
                r = api.get_ticker(symbol=market['market'])
                ticker = float(r.get('lastPrice', None))
                balance = float(api.get_asset_balance(market['asset']).get('free', 0))
                print('%s: %s' % (market['market'], ticker))

                if market.get('opening', False) and ticker <= market['opening_threshold'] and \
                        balance < market['max_asset_value']:
                    # Open new position:
                    print('Opening %s, amount: %s and lastPrice: %s' % (
                        market['market'], market['opening_usdt_amount'], ticker))

                    r = api.create_order(
                        symbol=market['market'],
                        side=SIDE_BUY,
                        type=ORDER_TYPE_MARKET,
                        quantity=market['opening_usdt_amount'])
                    print('.. order details: %s' % r)

                if ticker >= market['closing_threshold']:
                    if balance > 0:
                        # Close on negative valuation
                        print('Closing all positions %s, amount: %s and lastPrice: %s' % (
                            market['market'], balance, ticker))

                        r = api.create_order(
                            symbol=market['market'],
                            side=SIDE_SELL,
                            type=ORDER_TYPE_MARKET,
                            quantity=balance)
                        print('.. order details: %s' % r)
            except Exception as e:
                print("%s - Error in loop 1 with market %s: %s" % (
                    dt.datetime.now(), market['market'], e))

        time.sleep(SLEEP_SECONDS)

except Exception as e:
    print("%s - Error: %s" % (dt.datetime.now(), e))
finally:
    print("%s - Stopped" % dt.datetime.now())
