"""
This script rolls back a selling or buying order:
 - Opening order will get removed from positions collection
 - Closing order will get back to Open status in positions collection
"""
import yaml
import copy
import argparse
import datetime as dt
import time
from pymongo import MongoClient

from binance.client import Client as Binance
from binance.enums import *

parser = argparse.ArgumentParser(description='This script rolls back a selling or buying order')
parser.add_argument('--order-id', type=int, required=True,
                    help='Order to rollback')
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
    db = mongo.dumbot

    # Exchange API keys
    API_KEY = config.get('%s_api_key' % exchange, None)
    API_SECRET = config.get('%s_api_secret' % exchange, None)

    # Initialize exchange api
    api = Binance(API_KEY, API_SECRET)
    # Is binance alive ?
    if api.get_system_status().get("status", -1) != 0:
        raise Exception("Exchange unavailable for trading")

    # Get position details
    position = db.positions.find_one({"open_order_id": args.order_id})
    if position is None:
        raise Exception("Order %s not found !" % args.order_id)
    if position['status'] not in ['opening', 'closing']:
        raise Exception("This is not an 'ing' order !")

    # Show position to user
    print("* #%s %s (%s) since %s:\n"
          "\t- open at %s USDT, now at %s USDT (last update: %s)\n"
          "\t- initial volume %s, remaining: %s\n" % (
              position['open_order_id'],
              position['market'],
              position['status'],
              position['open_at'],
              position['open_rate'],
              position['current_price'],
              position['price_at'],
              position['volume'],
              position['remaining_volume'],
    ))

    # Can we cancel ?
    print("# Please confirm canceling order #%s ? (y/n)" % position['open_order_id'])
    choice = str(input())
    if choice.lower() != 'y':
        raise Exception("Cancelled")

    # If position were in 'opening' status, then simply clear it from db
    if position['status'] == 'opening':
        # Cancel on binance
        r = api.cancel_order(symbol=position['market'], orderId=position['open_order_id'])
        if r.get('status') != 'CANCELED':
            raise Exception("Cannot cancel order on Binance, result: %s" % r)

        db.positions.delete_one({'_id': position['_id']})
    else:
        # Cancel on binance
        r = api.cancel_order(symbol=position['market'], orderId=position['close_order_id'])
        if r.get('status') != 'CANCELED':
            raise Exception("Cannot cancel order on Binance, result: %s" % r)

        db.positions.update_one({'_id': position['_id']}, {
            '$set': {
                'status': 'open',
                'closure_reason': '%s => CANCELLED on %s' % (position['closure_reason'], dt.datetime.utcnow()),
                'closed_at': None,
                'last_update_at': dt.datetime.utcnow(),
            }})

    print("Succesfully canceled order #%s" % position['open_order_id'])
except Exception as e:
    print("Error: %s" % e)
finally:
    print("Stopped")
