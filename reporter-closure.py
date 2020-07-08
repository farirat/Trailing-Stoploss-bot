"""
This script pushes pair performance stats on closure to DB
"""
import yaml
import argparse
import datetime as dt
import time
from pymongo import MongoClient

from binance.client import Client as Binance
from binance.enums import *

parser = argparse.ArgumentParser(description='Calculates trading stats per pair on closure and persist '
                                             'them to reports_closure'
                                             ' collection')
parser.add_argument('--exchange', choices=['bittrex', 'binance'], required=True,
                    help='Exchange to use')
parser.add_argument('--config', type=str, required=False, default="config.yml",
                    help='Config file')

args = parser.parse_args()

try:
    if args.exchange != 'binance':
        raise NotImplementedError("Reporter is only implemeted for Binance exchanges")

    # Load configuration
    config = yaml.load(open(args.config, 'r'), Loader=yaml.SafeLoader)

    # Initialize mongo api
    mongo = MongoClient(config.get('db', None))
    mongo.server_info()
    db = mongo.dumbot

    # Exchange API keys
    API_KEY = config.get('%s_api_key' % args.exchange, None)
    API_SECRET = config.get('%s_api_secret' % args.exchange, None)
    SLEEP_SECONDS = 5

    # Initialize exchange api
    api = Binance(API_KEY, API_SECRET)
    # Is binance alive ?
    if api.get_system_status().get("status", -1) != 0:
        raise Exception("Exchange unavailable for trading")

    # Get market_settings
    markets = {}
    _skeleton = {
        'closed_last_hour': 0,
        'cumulated_gain_last_hour': 0,
        'gain_at_stoploss': 0,
        'open_positions': 0,
        'opening_positions': 0,
        'closing_positions': 0,
        'closed_positions': 0,
        '24h_gain': 0,
        '1w_gain': 0,
        '1m_gain': 0,
        '3m_gain': 0,
        '6m_gain': 0,
        '1y_gain': 0,
        'cumulated_gain': 0,
    }
    for _o in db.market_settings.find({"reporting": True}):
        markets[_o['market']] = _skeleton
    markets['other'] = _skeleton

    # Get all closed positions in last hour
    right = dt.datetime.utcnow()
    left = right - dt.timedelta(minutes=60)
    #_left = dt.datetime(2020, 1, 23, 10, 0,0)
    for position in db.positions.find({"$and": [
        {"status": "closed"},
        {"closed_at": {"$gt": left, "$lte": right}}
    ]}):
        # Get gain at stop loss for this market
        cursor = db.positions.aggregate([
            {"$match": {'status': 'open', 'market': position['market']}},
            {"$project": {
                "stop_loss_value": {"$divide": [{"$multiply": ["$stop_loss_percent", "$open_cost_proceeds"]}, 100]}
            }},
            {"$group": {
                "_id": None,
                "sum": {"$sum": "$stop_loss_value"}
            }}
        ]);
        _res = list(cursor)
        if len(_res) > 0:
            _gain_at_stop_loss = _res[0].get('sum')
        else:
            _gain_at_stop_loss = 0

        # Get gain for this market
        cursor = db.positions.aggregate([
            {"$match": {'status': 'closed', 'market': position['market']}},
            {"$group": {
                "_id": None,
                "sum": {"$sum": "$net"}
            }}
        ]);
        _res = list(cursor)
        if len(_res) > 0:
            _cumulated_gain = _res[0].get('sum')
        else:
            _cumulated_gain = 0

        # Get 24h gain for this market
        _left = right - dt.timedelta(hours=24)
        cursor = db.positions.aggregate([
            {"$match": {'status': 'closed',
                        'market': position['market'],
                        'closed_at': {"$gt": _left, "$lte": right}}},
            {"$group": {
                "_id": None,
                "sum": {"$sum": "$net"}
            }}
        ]);
        _res = list(cursor)
        if len(_res) > 0:
            _24h_gain = _res[0].get('sum')
        else:
            _24h_gain = 0

        # Get 1w gain for this market
        _left = right - dt.timedelta(days=7)
        cursor = db.positions.aggregate([
            {"$match": {'status': 'closed',
                        'market': position['market'],
                        'closed_at': {"$gt": _left, "$lte": right}}},
            {"$group": {
                "_id": None,
                "sum": {"$sum": "$net"}
            }}
        ]);
        _res = list(cursor)
        if len(_res) > 0:
            _1w_gain = _res[0].get('sum')
        else:
            _1w_gain = 0

        # Get 1m gain for this market
        _left = right - dt.timedelta(days=31)
        cursor = db.positions.aggregate([
            {"$match": {'status': 'closed',
                        'market': position['market'],
                        'closed_at': {"$gt": _left, "$lte": right}}},
            {"$group": {
                "_id": None,
                "sum": {"$sum": "$net"}
            }}
        ]);
        _res = list(cursor)
        if len(_res) > 0:
            _1m_gain = _res[0].get('sum')
        else:
            _1m_gain = 0

        # Get 3m gain for this market
        _left = right - dt.timedelta(days=93)
        cursor = db.positions.aggregate([
            {"$match": {'status': 'closed',
                        'market': position['market'],
                        'closed_at': {"$gt": _left, "$lte": right}}},
            {"$group": {
                "_id": None,
                "sum": {"$sum": "$net"}
            }}
        ]);
        _res = list(cursor)
        if len(_res) > 0:
            _3m_gain = _res[0].get('sum')
        else:
            _3m_gain = 0

        # Get 6m gain for this market
        _left = right - dt.timedelta(days=186)
        cursor = db.positions.aggregate([
            {"$match": {'status': 'closed',
                        'market': position['market'],
                        'closed_at': {"$gt": _left, "$lte": right}}},
            {"$group": {
                "_id": None,
                "sum": {"$sum": "$net"}
            }}
        ]);
        _res = list(cursor)
        if len(_res) > 0:
            _6m_gain = _res[0].get('sum')
        else:
            _6m_gain = 0

        # Get 1y gain for this market
        _left = right - dt.timedelta(days=365)
        cursor = db.positions.aggregate([
            {"$match": {'status': 'closed',
                        'market': position['market'],
                        'closed_at': {"$gt": _left, "$lte": right}}},
            {"$group": {
                "_id": None,
                "sum": {"$sum": "$net"}
            }}
        ]);
        _res = list(cursor)
        if len(_res) > 0:
            _1y_gain = _res[0].get('sum')
        else:
            _1y_gain = 0

        # Get position counts for this market
        _open_positions = db.positions.count_documents({'status': 'open', 'market': position['market']})
        _opening_positions = db.positions.count_documents({'status': 'opening', 'market': position['market']})
        _closing_positions = db.positions.count_documents({'status': 'closing', 'market': position['market']})
        _closed_positions = db.positions.count_documents({'status': 'closed', 'market': position['market']})

        if position['market'] in markets:
            _key = position['market']
        else:
            _key = 'other'

        markets[_key] = {
            'closed_last_hour': markets[_key]['closed_last_hour'] + 1,
            'cumulated_gain_last_hour': markets[_key]['cumulated_gain_last_hour'] + position['net'],
            'gain_at_stoploss': markets[_key]['gain_at_stoploss'] + _gain_at_stop_loss,
            'open_positions': _open_positions,
            'opening_positions': _opening_positions,
            'closing_positions': _closing_positions,
            'closed_positions': _closed_positions,
            '24h_gain': _24h_gain,
            '1w_gain': _1w_gain,
            '1m_gain': _1m_gain,
            '3m_gain': _3m_gain,
            '6m_gain': _6m_gain,
            '1y_gain': _1y_gain,
            'cumulated_gain': markets[_key]['cumulated_gain'] + _cumulated_gain,
        }

    # Do not store zero values
    # Cleansing:
    for market, data in list(markets.items()):
        if (markets[market]['closed_last_hour'] == 0 and markets[market]['open_positions'] == 0
            and markets[market]['opening_positions'] == 0 and markets[market]['closing_positions'] == 0):
            del(markets[market])

    _doc = {
        "created_at": dt.datetime.utcnow(),
        "from_datetime": left,
        "to_datetime": right,
        "pairs": markets
    }

    db.reports_closures.insert_one(_doc)
except Exception as e:
    print("Error: %s" % e)
finally:
    print("Stopped")
