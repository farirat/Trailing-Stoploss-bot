"""Lookup in-progress orders (opening or closing) and update their statuses in the db
"""
import datetime as dt
import yaml
import argparse
import time
from pymongo import MongoClient

from bittrex.bittrex import Bittrex, API_V2_0, API_V1_1

from binance.client import Client as Binance
from binance.enums import *

parser = argparse.ArgumentParser(description='Order synchronization bot.')
parser.add_argument('--exchange', choices=['bittrex', 'binance'], required=True,
                    help='Exchange to use')
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
    SLEEP_SECONDS = 30

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

    while True:
        ticker_cache = {}
        for position in db.positions.find({"$and":[
            {"status": {"$in": ["opening", "closing"]}},
            {"broker": args.exchange}
        ]}):
            try:
                print(" > [%s] %s %s (%s)" % (
                    args.exchange, position.get('_id'), position.get('market'), position.get('status')))

                # Get order_id
                order_id = position.get('open_order_id') if position.get('status') == 'opening' \
                    else position.get('close_order_id')

                # Get order status from broker
                if args.exchange == 'bittrex':
                    r = api.get_order(order_id)
                    if not r.get('success', False):
                        raise Exception("Cannot get order %s: %s" % (order_id, r))
                    order_price = r.get('result', {}).get('Price', 0)
                    order_type = r.get('result', {}).get('Type', None)
                    order_is_open = r.get('result', {}).get('IsOpen', False)
                    order_remaining_quantity = r.get('result', {}).get('QuantityRemaining', 0)
                    order_cancel_initiated = r.get('result', {}).get('CancelInitiated', False)
                    order_commission_paid = r.get('result', {}).get('CommissionPaid', 0)
                elif args.exchange == 'binance':
                    r = api.get_order(symbol=position.get('market'), orderId=order_id)
                    if r.get('orderId', None) != order_id or 'type' not in r:
                        raise Exception("Cannot get order %s: %s" % (order_id, r))
                    order_price = float(r.get('cummulativeQuoteQty', 0))
                    order_type = '%s_%s' % (r.get('type', 'ND'), r.get('side', 'ND'))
                    order_is_open = True if r.get('status', False) in ['PARTIALLY_FILLED', 'PENDING_CANCEL', 'NEW'] else False
                    order_remaining_quantity = float(r.get('origQty', 0)) - float(r.get('executedQty', 0))
                    order_cancel_initiated = r.get('PENDING_CANCEL', False)
                    # @TODO: Will not calculate commission with Binance because of BNB fees complexity
                    order_commission_paid = 0

                # We handle only LIMIT orders
                if order_type not in ['LIMIT_BUY', 'LIMIT_SELL']:
                    raise Exception("Order type rejected for this position: %s" % order_type)

                # Get ticker value
                if position.get('market') not in ticker_cache:
                    if args.exchange == 'bittrex':
                        r = api.get_ticker(position.get('market'))
                        ticker_cache[position.get('market')] = r.get('result', {}).get('Last', None)
                    elif args.exchange == 'binance':
                        r = api.get_ticker(symbol=position.get('market'))
                        ticker_cache[position.get('market')] = r.get('lastPrice', None)
                    if ticker_cache[position.get('market')] is None:
                        print("Cannot get last ticker value for %s" % (position.get('market')))
                        continue
                    else:
                        ticker_cache[position.get('market')] = float(ticker_cache[position.get('market')])

                _LAST_TICKER_VALUE = ticker_cache[position.get('market')]

                # Are we still in an 'ing' status ?
                if order_is_open:
                    db.positions.update_one({'_id': position.get('_id')}, {
                        '$set': {
                            'remaining_volume': order_remaining_quantity,
                            'current_price': _LAST_TICKER_VALUE,
                            'price_at': dt.datetime.utcnow(),
                            'last_update_at': dt.datetime.utcnow(),
                        }})
                else:
                    paid_commission = position.get('paid_commission', 0) + order_commission_paid
                    if not order_cancel_initiated:
                        # Order complete:
                        #########################################
                        db.positions.update_one({'_id': position.get('_id')}, {
                            '$set': {
                                'status': 'open' if order_type == 'LIMIT_BUY' else 'closed',
                                'paid_commission': paid_commission,
                                'remaining_volume': order_remaining_quantity,
                                'last_update_at': dt.datetime.utcnow(),
                            }})

                        if order_type == 'LIMIT_SELL':
                            # If we're closing then update the net
                            _close_cost_proceeds = order_price - order_commission_paid
                            _net = _close_cost_proceeds - position.get('open_cost_proceeds', 0)
                            _net_percent = ((_close_cost_proceeds * 100) / position.get('open_cost_proceeds', 0)) - 100
                            db.positions.update_one({'_id': position.get('_id')}, {
                                '$set': {
                                    'fully_closed_at': dt.datetime.utcnow(),
                                    'close_commission': order_commission_paid,
                                    'close_cost': order_price,
                                    'close_cost_proceeds': _close_cost_proceeds,
                                    'net': _net,
                                    'net_percent': _net_percent,
                                    'last_update_at': dt.datetime.utcnow(),
                                }})
                        else:
                            # If we're opening then update the open_costs
                            _open_cost_proceeds = order_price + order_commission_paid
                            db.positions.update_one({'_id': position.get('_id')}, {
                                '$set': {
                                    'fully_open_at': dt.datetime.utcnow(),
                                    'open_commission': order_commission_paid,
                                    'open_cost': order_price,
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
                                'remaining_volume': order_remaining_quantity,
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
