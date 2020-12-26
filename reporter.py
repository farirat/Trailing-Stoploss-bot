"""
This script pushes trading stats to DB
"""
import yaml
import argparse
import datetime as dt
import time
from pymongo import MongoClient

from bittrex.bittrex import Bittrex, API_V2_0, API_V1_1

from binance.client import Client as Binance
from binance.enums import *

parser = argparse.ArgumentParser(description='Calculates trading stats and persist them to reports collection')
parser.add_argument('--exchange', choices=['bittrex', 'binance'], required=True,
                    help='Exchange to use')
parser.add_argument('--config', type=str, required=False, default="config.yml",
                    help='Config file')

args = parser.parse_args()

try:
    if args.exchange != 'binance':
        raise NotImplementedError("Reported is only implemeted for Binance exchanges")

    # Load configuration
    config = yaml.load(open(args.config, 'r'), Loader=yaml.SafeLoader)

    # Initialize mongo api
    mongo = MongoClient(config.get('db', None))
    mongo.server_info()
    db = mongo[config.get('db_name', 'dumbot')]

    # Exchange API keys
    API_KEY = config.get('%s_api_key' % args.exchange, None)
    API_SECRET = config.get('%s_api_secret' % args.exchange, None)

    # Initialize exchange api
    if args.exchange == 'bittrex':
        api = Bittrex(API_KEY, API_SECRET, api_version=API_V1_1)
    elif args.exchange == 'binance':
        api = Binance(API_KEY, API_SECRET)

        # Is binance alive ?
        if api.get_system_status().get("status", -1) != 0:
            raise Exception("Exchange unavailable for trading")
    else:
        raise NotImplementedError

    # Get user balance
    r = api.get_asset_balance(asset='USDT')
    if r.get('asset', None) != 'USDT':
        raise Exception("Cant get USDT Balance: %s" % r)
    _available = r['free']
    _locked = r['locked']

    # Get gain
    cursor = db.positions.aggregate([
        {"$match": {'status': 'closed'}},
        {"$group": {
            "_id" : None,
            "sum": {"$sum": "$net"}
        }}
    ]);
    _cumulated_gain = list(cursor)[0].get('sum')

    # Get gain at stop loss
    cursor = db.positions.aggregate([
        {"$match": {'status': 'open'}},
        {"$project": {
            "stop_loss_value": {"$divide": [{"$multiply": ["$stop_loss_percent", "$open_cost_proceeds"]}, 100]}
        }},
        {"$group": {
            "_id": None,
            "sum": {"$sum": "$stop_loss_value"}
        }}
    ]);
    _gain_at_stop_loss = list(cursor)[0].get('sum')

    # Get expected Gain value now
    cursor = db.positions.aggregate([
        {"$match": {'status': 'open'}},
        {"$group": {
            "_id": None,
            "sum": {"$sum": "$expected_net"}
        }}
    ]);
    _gain_now = list(cursor)[0].get('sum')

    # Get position counts
    _open_positions = db.positions.count_documents({'status': 'open'})
    _opening_positions = db.positions.count_documents({'status': 'opening'})
    _closing_positions = db.positions.count_documents({'status': 'closing'})
    _closed_positions = db.positions.count_documents({'status': 'closed'})

    # Get investment value
    cursor = db.positions.aggregate([
        {"$match": {'status': 'open'}},
        {"$group": {
            "_id": None,
            "sum": {"$sum": "$open_cost_proceeds"}
        }}
    ]);
    _balance = list(cursor)[0].get('sum')
    _equity = _balance + _gain_now

    # Calculate drawdaw
    if _equity < _balance:
        _drawdown = 100 - (_equity*100/_balance)
    else:
        _drawdown = None

    _doc = {
        "created_at": dt.datetime.utcnow(),
        "cumulated_gains": _cumulated_gain,
        "gain_at_stop_loss": _gain_at_stop_loss,
        "gain_now": _gain_now,
        "open_positions": _open_positions,
        "opening_positions": _opening_positions,
        "closing_positions": _closing_positions,
        "closed_positions": _closed_positions,
        "balance": _balance,
        "equity": _equity,
        "available": _available,
        "locked": _locked,
    }
    if _drawdown is not None:
        _doc['drawdown'] = _drawdown

    db.reports.insert_one(_doc)
except Exception as e:
    print("Error: %s" % e)
finally:
    print("Stopped")
