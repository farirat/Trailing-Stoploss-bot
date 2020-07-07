"""
This script will open a new position no matter the price.

It is based on the market_settings collection
"""
import yaml
import argparse
import time
import datetime as dt
from pymongo import MongoClient
from crontab import CronTab

from binance.client import Client as Binance
from binance.enums import *

parser = argparse.ArgumentParser(description='Exchange buyer bot based on market_settings collection.')
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
    db = mongo.dumbot

    # Exchange API keys
    API_KEY = config.get('%s_api_key' % exchange, None)
    API_SECRET = config.get('%s_api_secret' % exchange, None)

    locked_markets = {}
    while True:
        # Clean expired locked markets
        for locked_market, data in list(locked_markets.items()):
            if data['locked_until'] < dt.datetime.utcnow():
                del(locked_markets[locked_market])

        open_queue = []
        # Fill the open_queue
        for market in db.market_settings.find({"trading": True}):
            try:
                if 'opening_schedule' not in market:
                    continue
                entry = CronTab(market['opening_schedule'])

                if entry.next(default_utc=True) < 60 and market['market'] not in locked_markets:
                    print("%s - %s hit !" % (dt.datetime.now(), market['market']))
                    open_queue.append(market)
            except Exception as e:
                print("%s - Error in loop 1 with market %s: %s" % (dt.datetime.now(), market['market'], e))

        if len(open_queue) > 0:
            # Re-Initialize binance api
            api = Binance(API_KEY, API_SECRET)

            # Is binance alive ?
            if api.get_system_status().get("status", -1) != 0:
                raise Exception("Exchange unavailable for trading")

            # Execute open queue
            for market in open_queue:
                try:
                    # Open position logic:
                    # 1. Get market last price
                    r = api.get_ticker(symbol=market['market'])
                    ticker = float(r.get('lastPrice', None))

                    # 1'. Get market limits and parameters (binance specific)
                    market_info = api.get_symbol_info(market['market'])
                    market_filters = {}
                    for _f in market_info.get('filters', {}):
                        market_filters[_f['filterType']] = _f

                    if 'opening_usdt_amount' in market:
                        # 2. Buy with args.total value
                        # Calculate the _quantity in respect to LOT_SIZE filter (binance specific) then make it compliant to stepSize
                        _quantity = market['opening_usdt_amount'] / ticker
                        _LOT_SIZE_maxQty = float(market_filters['LOT_SIZE']['maxQty'])
                        _LOT_SIZE_minQty = float(market_filters['LOT_SIZE']['minQty'])
                        _LOT_SIZE_stepSize = float(market_filters['LOT_SIZE']['stepSize'])
                        _quantity -= ((_quantity - _LOT_SIZE_minQty) % _LOT_SIZE_stepSize)
                        _quantity = float(format(_quantity, '.%sf' % market_info.get('baseAssetPrecision', 2)))
                        _rate = ticker
                        r = api.create_order(
                            symbol=market['market'],
                            side=SIDE_BUY,
                            type=ORDER_TYPE_LIMIT,
                            timeInForce=TIME_IN_FORCE_GTC,
                            quantity=_quantity,
                            price=_rate)
                        if r.get('status', None) not in ['PARTIALLY_FILLED', 'NEW', 'FILLED'] or r.get('orderId', None) is None:
                            raise Exception("Could not open position on broker: %s" % r)
                        open_order_id = r.get('orderId')

                        print("%s New position %s %s @ %s: %s" % (
                            dt.datetime.now(),
                            _quantity,
                            market['market'],
                            _rate,
                            open_order_id
                        ))

                        _doc = {
                            "open_at": dt.datetime.utcnow(),
                            "status": "opening",
                            "market": market['market'],
                            "open_order_id": open_order_id,
                            "broker": exchange,
                            "open_rate": _rate,
                            "volume": _quantity,
                            "current_price": _rate,
                            "price_at": dt.datetime.utcnow(),
                            'last_update_at': dt.datetime.utcnow(),
                        }
                        db.positions.insert_one(_doc)

                        # Lock this market for 5 minutes to avoid multiple openings in very short time
                        locked_markets[market['market']] = {'locked_until': dt.datetime.utcnow() + dt.timedelta(minutes=5)}
                except Exception as e:
                    print("%s - Error in loop 2 with market %s: %s" % (dt.datetime.now(), market['market'], e))

        time.sleep(10)

except Exception as e:
    print("%s - Error: %s" % (dt.datetime.now(), e))
finally:
    print("%s - Stopped" % dt.datetime.now())
