"""
This script pushes pair performance stats to DB
"""
import yaml
import argparse
import datetime as dt
import time
from pymongo import MongoClient

from binance.client import Client as Binance
from binance.enums import *

parser = argparse.ArgumentParser(description='Calculates trading stats per pair and persist them to reports_pairperf'
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

    # Get pairperf_column_settings
    markets = {}
    for _o in db.pairperf_column_settings.find():
        markets[_o['market']] = {
            'closed_positions': 0,
            'gain_at_stoploss': 0,
            'cumulated_gains': 0
        }
    markets['other'] = {
            'closed_positions': 0,
            'gain_at_stoploss': 0,
            'cumulated_gains': 0
        }

    # Get all closed positions in last hour
    now = dt.datetime.utcnow()
    for position in db.positions.find({"$and": [
        {"status": "closed"},
        {"closed_at": {"$gt": (now - dt.timedelta(minutes=60)), "$lte": now}}
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
        _gain_at_stop_loss = list(cursor)[0].get('sum')

        if position['market'] in markets:
            markets[position['market']] = {
                'closed_positions': markets[position['market']]['closed_positions'] + 1,
                'gain_at_stoploss': markets[position['market']]['gain_at_stoploss'] + _gain_at_stop_loss,
                'cumulated_gains': markets[position['market']]['cumulated_gains'] + position['net']
            }
        else:
            markets['other'] = {
                'closed_positions': markets['other']['closed_positions'] + 1,
                'gain_at_stoploss': markets['other']['gain_at_stoploss'] + _gain_at_stop_loss,
                'cumulated_gains': markets['other']['cumulated_gains'] + position['net']
            }

    # Summarize stats per closed pair in last hour

    _doc = {
        "created_at": dt.datetime.utcnow(),
        "pairs": markets
    }

    db.reports_pairperf.insert_one(_doc)
except Exception as e:
    print("Error: %s" % e)
finally:
    print("Stopped")
