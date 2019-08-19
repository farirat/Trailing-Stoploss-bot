"""
This script will open a new position no matter the price.
"""
import yaml
import argparse
import datetime as dt
from pymongo import MongoClient
from bittrex.bittrex import Bittrex, API_V2_0, API_V1_1

parser = argparse.ArgumentParser(description='Bittrex buyer bot.')
parser.add_argument('--market-base', type=str, required=True,
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

    # Bittrex API
    API_KEY = config.get('bittrex_api_key', None)
    API_SECRET = config.get('bittrex_api_secret', None)

    # Initialize bittrex api
    api = Bittrex(API_KEY, API_SECRET, api_version=API_V1_1)

    # Initialize mongo api
    mongo = MongoClient(config.get('db', None))
    mongo.server_info()
    db = mongo.dumbot

    # Open position logic:
    # 1. Get market last ask price
    r = api.get_ticker("%s-%s" % (args.market_base, args.market_currency))
    if not r.get('success', False):
        raise Exception("Got an error while querying broker: %s" % r.get('message', 'nd'))
    ticker = r.get('result')

    # 2. Buy with args.total value
    _quantity = args.total / ticker.get('Ask', 0)
    _rate = ticker.get('Ask', 0)
    r = api.buy_limit("%s-%s" % (args.market_base, args.market_currency),
                      quantity=_quantity, rate=_rate)
    if not r.get('success', False):
        raise Exception("Could not open position on broker: %s" % r.get('message', 'nd'))

    open_order_id = r.get('result', {}).get('uuid', None)
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
        "market": "%s-%s" % (args.market_base, args.market_currency),
        "open_order_id": open_order_id,
        "broker": "bittrex",
        "open_rate": _rate,
        "volume": _quantity,
        "current_price": ticker.get('Last', 0),
        "price_at": dt.datetime.utcnow(),
        'last_update_at': dt.datetime.utcnow(),
    }
    db.positions.insert_one(_doc)
except Exception as e:
    print("Error: %s" % e)
finally:
    print("Stopped")
