"""
This script will open a new position no matter the price.
"""
import yaml
import argparse
import datetime as dt
from pymongo import MongoClient

from bittrex.bittrex import Bittrex, API_V2_0, API_V1_1

from binance.client import Client as Binance
from binance.enums import *

parser = argparse.ArgumentParser(description='Exchange buyer bot.')
parser.add_argument('--exchange', choices=['bittrex', 'binance'], required=True,
                    help='Exchange to use')
parser.add_argument('--market-base', type=str, default='USDT',
                    help='Market base (ex: USD for market USD-BTC)')
parser.add_argument('--market-currency', type=str, required=True,
                    help='Market base (ex: BTC for market USD-BTC)')
parser.add_argument('--total', type=float, required=True,
                    help='Total amount to pay in marker-base currency')
parser.add_argument('--config', type=str, required=False, default="config.yml",
                    help='Config file')

args = parser.parse_args()

try:
    # Load configuration
    config = yaml.load(open(args.config, 'r'), Loader=yaml.SafeLoader)

    # Initialize mongo api
    mongo = MongoClient(config.get('db', None))
    mongo.server_info()
    db = mongo[self.config.get('db_name', 'dumbot')]

    # Exchange API keys
    API_KEY = config.get('%s_api_key' % args.exchange, None)
    API_SECRET = config.get('%s_api_secret' % args.exchange, None)

    if args.exchange == 'bittrex':
        market = "%s-%s" % (args.market_base, args.market_currency)

        # Initialize bittrex api
        api = Bittrex(API_KEY, API_SECRET, api_version=API_V1_1)

        # Open position logic:
        # 1. Get market last ask price
        r = api.get_ticker(market)
        if not r.get('success', False):
            raise Exception("Got an error while querying broker: %s" % r.get('message', 'nd'))
        ticker = r.get('result')

        # 2. Buy with args.total value
        _quantity = args.total / ticker.get('Ask', 0)
        _rate = ticker.get('Ask', 0)
        r = api.buy_limit(market, quantity=_quantity, rate=_rate)
        if not r.get('success', False):
            raise Exception("Could not open position on broker: %s" % r.get('message', 'nd'))

        open_order_id = r.get('result', {}).get('uuid', None)
    elif args.exchange == 'binance':
        market = "%s%s" % (args.market_currency, args.market_base)

        # Initialize binance api
        api = Binance(API_KEY, API_SECRET)

        # Is binance alive ?
        if api.get_system_status().get("status", -1) != 0:
            raise Exception("Exchange unavailable for trading")

        # Open position logic:
        # 1. Get market last price
        r = api.get_ticker(symbol=market)
        ticker = float(r.get('lastPrice', None))

        # 1'. Get market limits and parameters (binance specific)
        market_info = api.get_symbol_info(market)
        market_filters = {}
        for _f in market_info.get('filters', {}):
            market_filters[_f['filterType']] = _f

        # 2. Buy with args.total value
        # Calculate the _quantity in respect to LOT_SIZE filter (binance specific) then make it compliant to stepSize
        _quantity = args.total / ticker
        _LOT_SIZE_maxQty = float(market_filters['LOT_SIZE']['maxQty'])
        _LOT_SIZE_minQty = float(market_filters['LOT_SIZE']['minQty'])
        _LOT_SIZE_stepSize = float(market_filters['LOT_SIZE']['stepSize'])
        _quantity -= ((_quantity - _LOT_SIZE_minQty) % _LOT_SIZE_stepSize)
        _quantity = float(format(_quantity, '.%sf' % market_info.get('baseAssetPrecision', 2)))
        _rate = ticker
        r = api.create_order(
            symbol=market,
            side=SIDE_BUY,
            type=ORDER_TYPE_LIMIT,
            timeInForce=TIME_IN_FORCE_GTC,
            quantity=_quantity,
            price=_rate)
        if r.get('status', None) not in ['PARTIALLY_FILLED', 'NEW', 'FILLED'] or r.get('orderId', None) is None:
            raise Exception("Could not open position on broker: %s" % r)

        open_order_id = r.get('orderId')
    else:
        raise NotImplementedError

    print("New position %s%s @ %s%s: %s" % (
        _quantity,
        args.market_currency,
        _rate,
        args.market_base,
        open_order_id
    ))

    _doc = {
        "open_at": dt.datetime.utcnow(),
        "status": "opening",
        "market": market,
        "open_order_id": open_order_id,
        "broker": args.exchange,
        "open_rate": _rate,
        "volume": _quantity,
        "current_price": _rate,
        "price_at": dt.datetime.utcnow(),
        'last_update_at': dt.datetime.utcnow(),
    }
    db.positions.insert_one(_doc)
except Exception as e:
    print("Error: %s" % e)
finally:
    print("Stopped")
