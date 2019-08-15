"""Lookup in-progress orders (opening or closing) and update their statuses in the db
"""
import yaml
import argparse
import datetime as dt
import time
from pymongo import MongoClient
from bittrex.bittrex import Bittrex, API_V2_0, API_V1_1

parser = argparse.ArgumentParser(description='Order synchronization bot.')
parser.add_argument('--config', type=str, required=False, default="config.yml",
                    help='Config file')

args = parser.parse_args()

try:
    # Load configuration
    config = yaml.load(open(args.config, 'r'), Loader=yaml.SafeLoader)

    # Bittrex API
    API_KEY = config.get('bittrex_api_key', None)
    API_SECRET = config.get('bittrex_api_secret', None)
    SLEEP_SECONDS = 180

    # Initialize bittrex api
    api = Bittrex(API_KEY, API_SECRET, api_version=API_V1_1)

    # Initialize mongo api
    mongo = MongoClient(config.get('db', None))
    mongo.server_info()
    db = mongo.dumbot

    while True:
        for position in db.positions.find({"status": {"$in": ["opening", "closing"]}}):
            try:
                print(" > %s (%s)" % (
                    position.get('market'), position.get('status')))

                # Get order_id
                order_id = position.get('open_order_id') if position.get('status') == 'opening' \
                    else position.get('close_order_id')

                # Get order status from broker
                r = api.get_order(order_id)
                if not r.get('success', False):
                    raise Exception("Cannot get order %s: %s" % (order_id, r))

                # We handle only LIMIT orders
                order_type = r.get('result', {}).get('Type', None)
                if order_type not in ['LIMIT_BUY', 'LIMIT_SELL']:
                    raise Exception("Order type rejected for this position: %s" % order_type)

                # Are we still in an 'ing' status ?
                if r.get('result', {}).get('IsOpen', False):
                    db.positions.update_one({'_id': position.get('_id')}, {
                        '$set': {
                            'remaining_volume': r.get('result', {}).get('QuantityRemaining', 0)
                        }})
                else:
                    paid_commission = position.get('paid_commission', 0) + r.get('result', {}).get('CommissionPaid', 0)
                    db.positions.update_one({'_id': position.get('_id')}, {
                        '$set': {
                            'status': 'open' if order_type == 'LIMIT_BUY' else 'closed',
                            'paid_commission': paid_commission,
                            'remaining_volume': r.get('result', {}).get('QuantityRemaining', 0)
                        }})

                    # If we're closing then update the net
                    if order_type == 'LIMIT_SELL':
                        db.positions.update_one({'_id': position.get('_id')}, {
                            '$set': {
                                'net': 0,
                            }})

                    print(" > Order completed")
            except Exception as e:
                print("Error in position handling: %s" % e)
                continue

        time.sleep(SLEEP_SECONDS)
except Exception as e:
    print("Error: %s" % e)
finally:
    print("Stopped")
