"""
This script check for open positions and apply a trailing stoploss algorithm on each one
"""
import yaml
import argparse
import datetime as dt
import time
from pymongo import MongoClient

from bittrex.bittrex import Bittrex, API_V2_0, API_V1_1

from binance.client import Client as Binance
from binance.enums import *

parser = argparse.ArgumentParser(description='Automatic exchange trailing stoploss bot.')
parser.add_argument('--exchange', choices=['bittrex', 'binance'], required=True,
                    help='Exchange to use')
parser.add_argument('--stop-loss-percent', type=float, required=False, default=10,
                    help='Percentage of value decrease to trigger a stoploss action')
parser.add_argument('--dry-run', action='store_true',
                    help='If set, no sells will be placed.')
parser.add_argument('--config', type=str, required=False, default="config.yml",
                    help='Config file')

args = parser.parse_args()

STOPLOSS_PERCENTAGE = args.stop_loss_percent
DRY_RUN = args.dry_run

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
    SLEEP_SECONDS = 5

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
            {"status": "open"},
            {"broker": args.exchange}
        ]}):
            try:
                # Positions values
                POS_AMOUNT = position.get('volume')
                POS_BUY_PRICE = position.get('open_rate')

                # Init. stoppers configuration
                STOPLOSS_LIMIT = position.get('stop_loss', None)

                # Get ticker value
                if "%s" % (position.get('market')) not in ticker_cache:
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

                # Update the position information
                db.positions.update_one({'_id': position.get('_id')}, {
                    '$set': {
                        'current_price': _LAST_TICKER_VALUE,
                        'price_at': dt.datetime.utcnow(),
                        'last_update_at': dt.datetime.utcnow(),
                    }})

                # Recalculate the stoppers limits
                # Where:
                # - STOPLOSS will never get lower than previous iterations
                if _LAST_TICKER_VALUE > POS_BUY_PRICE:
                    _sl = _LAST_TICKER_VALUE - (_LAST_TICKER_VALUE * STOPLOSS_PERCENTAGE / 100)
                    if STOPLOSS_LIMIT is None or _sl > STOPLOSS_LIMIT:
                        STOPLOSS_LIMIT = _sl
                else:
                    _sl = POS_BUY_PRICE - (POS_BUY_PRICE * STOPLOSS_PERCENTAGE / 100)
                    if STOPLOSS_LIMIT is None or _sl > STOPLOSS_LIMIT:
                        STOPLOSS_LIMIT = _sl

                # Recalculate the net
                expected_net = (POS_AMOUNT * _LAST_TICKER_VALUE) - (POS_AMOUNT * POS_BUY_PRICE)
                expected_net_percent = (((POS_AMOUNT * _LAST_TICKER_VALUE) * 100) / (POS_AMOUNT * POS_BUY_PRICE)) - 100
                stop_loss_percent = (((POS_AMOUNT * STOPLOSS_LIMIT) * 100) / (POS_AMOUNT * POS_BUY_PRICE)) - 100
                db.positions.update_one({'_id': position.get('_id')}, {
                    '$set': {
                        'stop_loss_percent': stop_loss_percent,
                        'stop_loss': STOPLOSS_LIMIT,
                        'expected_net': expected_net,
                        'expected_net_percent': expected_net_percent,
                        'last_update_at': dt.datetime.utcnow(),
                    }})
                print(" > %s Last:%s, Stop loss @%s" % (
                    position.get('market'), _LAST_TICKER_VALUE, STOPLOSS_LIMIT))

                # If limits are defined and reached then we may close positions
                closure_reason = None
                if STOPLOSS_LIMIT is not None and _LAST_TICKER_VALUE <= STOPLOSS_LIMIT:
                    closure_reason = 'stoploss'

                # Get the hell out of here, we closed the position
                if closure_reason is not None:
                    print(" > Closing position %s %s@%s on %s @%s, expected_net:%s" % (
                        position.get('market'), POS_AMOUNT, POS_BUY_PRICE,
                        closure_reason, _LAST_TICKER_VALUE, expected_net))

                    if not DRY_RUN and not position.get('hodl', False):
                        if args.exchange == 'bittrex':
                            r = api.sell_limit("%s" % position.get('market'),
                                               quantity=POS_AMOUNT, rate=_LAST_TICKER_VALUE)
                            if not r.get('success', False):
                                raise Exception("Could not close position on broker: %s" % r)
                            close_order_id = r.get('result', {}).get('uuid', None)
                        elif args.exchange == 'binance':
                            r = api.order_limit_sell(symbol="%s" % position.get('market'),
                                               quantity=POS_AMOUNT, price=_LAST_TICKER_VALUE)
                            if r.get('status', None) not in ['PARTIALLY_FILLED', 'NEW', 'FILLED'] or r.get('orderId',
                                                                                                           None) is None:
                                raise Exception("Could not close position on broker: %s" % r)
                            close_order_id = r.get('orderId')

                        db.positions.update_one({'_id': position.get('_id')}, {
                            '$set': {
                                'status': 'closing',
                                'close_order_id': close_order_id,
                                'closure_reason': closure_reason,
                                'close_rate': _LAST_TICKER_VALUE,
                                'closed_at': dt.datetime.utcnow(),
                                'last_update_at': dt.datetime.utcnow(),
                            }})
                    else:
                        print(" > DRY_RUN mode: position not closed (hodl:%s)." % position.get('hodl', False))
                    continue
            except Exception as e:
                print("Error in position handling: %s" % e)
                continue

        time.sleep(SLEEP_SECONDS)
except Exception as e:
    print("Error: %s" % e)
finally:
    print("Stopped")
