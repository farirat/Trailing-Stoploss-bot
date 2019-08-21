"""Lookup in-progress orders (opening or closing) and update their statuses in the db
"""
import datetime as dt
import yaml
import argparse
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
        ticker_cache = {}
        for position in db.positions.find({"status": {"$in": ["opening", "closing"]}}):
            try:
                print(" > %s %s (%s)" % (
                    position.get('_id'), position.get('market'), position.get('status')))

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

                # Get ticker value
                if position.get('market') not in ticker_cache:
                    ticker_cache[position.get('market')] = api.get_ticker(
                        position.get('market')).get('result', {}).get('Last', None)
                    if ticker_cache[position.get('market')] is None:
                        print("Cannot get last ticker value for %s" % (position.get('market')))
                        continue
                _LAST_TICKER_VALUE = ticker_cache[position.get('market')]

                # Are we still in an 'ing' status ?
                if r.get('result', {}).get('IsOpen', False):
                    db.positions.update_one({'_id': position.get('_id')}, {
                        '$set': {
                            'remaining_volume': r.get('result', {}).get('QuantityRemaining', 0),
                            'current_price': _LAST_TICKER_VALUE,
                            'price_at': dt.datetime.utcnow(),
                            'last_update_at': dt.datetime.utcnow(),
                        }})
                else:
                    paid_commission = position.get('paid_commission', 0) + r.get('result', {}).get('CommissionPaid', 0)
                    if not r.get('result', {}).get('CancelInitiated', False):
                        # Order complete:
                        #########################################
                        db.positions.update_one({'_id': position.get('_id')}, {
                            '$set': {
                                'status': 'open' if order_type == 'LIMIT_BUY' else 'closed',
                                'paid_commission': paid_commission,
                                'remaining_volume': r.get('result', {}).get('QuantityRemaining', 0),
                                'last_update_at': dt.datetime.utcnow(),
                            }})

                        if order_type == 'LIMIT_SELL':
                            # If we're closing then update the net
                            _close_cost_proceeds = r.get('result', {}).get('Price', 0) + \
                                                   r.get('result', {}).get('CommissionPaid', 0)
                            _net = _close_cost_proceeds - position.get('open_cost_proceeds', 0)
                            _net_percent = ((_close_cost_proceeds * 100) / position.get('open_cost_proceeds', 0)) - 100
                            db.positions.update_one({'_id': position.get('_id')}, {
                                '$set': {
                                    'fully_closed_at': dt.datetime.utcnow(),
                                    'close_commission': r.get('result', {}).get('CommissionPaid', 0),
                                    'close_cost': r.get('result', {}).get('Price', 0),
                                    'close_cost_proceeds': _close_cost_proceeds,
                                    'net': _net,
                                    'net_percent': _net_percent,
                                    'last_update_at': dt.datetime.utcnow(),
                                }})
                        else:
                            # If we're opening then update the open_costs
                            _open_cost_proceeds = r.get('result', {}).get('Price', 0) + \
                                                  r.get('result', {}).get('CommissionPaid', 0)
                            db.positions.update_one({'_id': position.get('_id')}, {
                                '$set': {
                                    'fully_open_at': dt.datetime.utcnow(),
                                    'open_commission': r.get('result', {}).get('CommissionPaid', 0),
                                    'open_cost': r.get('result', {}).get('Price', 0),
                                    'open_cost_proceeds': _open_cost_proceeds,
                                    'last_update_at': dt.datetime.utcnow(),
                                }})
                    else:
                        # Order cancelled:
                        #########################################
                        db.positions.update_one({'_id': position.get('_id')}, {
                            '$set': {
                                'status': 'opening-cancelled' if order_type == 'LIMIT_BUY' else 'closing-cancelled',
                                'paid_commission': paid_commission,
                                'remaining_volume': r.get('result', {}).get('QuantityRemaining', 0),
                                'last_update_at': dt.datetime.utcnow(),
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
