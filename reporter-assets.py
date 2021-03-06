"""
This script keeps a reports_assets collection updated every hour
"""
import yaml
import copy
import argparse
import datetime as dt
import time
from pymongo import MongoClient

from binance.client import Client as Binance
from binance.enums import *

parser = argparse.ArgumentParser(description='Updates reports_assets collection '
                                             'with data from Binance api')
parser.add_argument('--config', type=str, required=False, default="config.yml",
                    help='Config file')

args = parser.parse_args()
exchange = 'binance'

try:
    if exchange != 'binance':
        raise NotImplementedError("Reporter is only implemeted for Binance exchanges")

    # Load configuration
    config = yaml.load(open(args.config, 'r'), Loader=yaml.SafeLoader)

    # Initialize mongo api
    mongo = MongoClient(config.get('db', None))
    mongo.server_info()
    db = mongo[config.get('db_name', 'dumbot')]

    # Exchange API keys
    API_KEY = config.get('%s_api_key' % exchange, None)
    API_SECRET = config.get('%s_api_secret' % exchange, None)
    SLEEP_SECONDS = 10

    # Initialize exchange api
    api = Binance(API_KEY, API_SECRET)
    # Is binance alive ?
    if api.get_system_status().get("status", -1) != 0:
        raise Exception("Exchange unavailable for trading")

    # Get asset details based on market_settings
    for _o in db.market_settings.find({"$or": [{"reporting": True}, {"trading": True}]}):
        # Get 24h ticker
        r = api.get_ticker(symbol=_o['market'])
        ticker = float(r.get('lastPrice', None))

        # Get asset
        asset_details = api.get_asset_balance(asset=_o['asset'])
        asset_details['last_updated_at'] = dt.datetime.utcnow()
        asset_details['locked'] = float(asset_details['locked'])
        asset_details['free'] = float(asset_details['free'])
        asset_details['locked_USDT'] = asset_details['locked'] * ticker
        asset_details['free_USDT'] = asset_details['free'] * ticker

        db.reports_assets.update_one({'asset': asset_details['asset']}, {"$set": asset_details}, upsert=True)

        # Slow down to avoid getting banned from the api
        time.sleep(SLEEP_SECONDS)

    # Get USDT asset details (manual)
    asset_details = api.get_asset_balance(asset='USDT')
    asset_details['last_updated_at'] = dt.datetime.utcnow()
    asset_details['locked'] = float(asset_details['locked'])
    asset_details['free'] = float(asset_details['free'])
    asset_details['locked_USDT'] = asset_details['locked']
    asset_details['free_USDT'] = asset_details['free']
    db.reports_assets.update_one({'asset': asset_details['asset']}, {"$set": asset_details}, upsert=True)
except Exception as e:
    print("Error: %s" % e)
finally:
    print("Stopped")
