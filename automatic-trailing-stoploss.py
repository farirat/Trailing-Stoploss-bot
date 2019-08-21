"""
This script check for open positions and apply a trailing stoploss algorithm on each one
"""
import yaml
import argparse
import datetime as dt
import time
from pymongo import MongoClient
from bittrex.bittrex import Bittrex, API_V2_0, API_V1_1

parser = argparse.ArgumentParser(description='Automatic Bittrex trailing stoploss bot.')
parser.add_argument('--stop-loss-percent', type=float, required=False, default=10,
                    help='Percentage of value decrease to trigger a stoploss action')
parser.add_argument('--stop-profit-percent', type=float, required=False, default=20,
                    help='Percentage of value increase to trigger a stopprofit action')
parser.add_argument('--dry-run', action='store_true',
                    help='If set, no sells will be placed.')
parser.add_argument('--config', type=str, required=False, default="config.yml",
                    help='Config file')

args = parser.parse_args()

STOPLOSS_PERCENTAGE = args.stop_loss_percent
STOPGAIN_PERCENTAGE = args.stop_profit_percent
DRY_RUN = args.dry_run

try:
    # Load configuration
    config = yaml.load(open(args.config, 'r'), Loader=yaml.SafeLoader)

    # Bittrex API
    API_KEY = config.get('bittrex_api_key', None)
    API_SECRET = config.get('bittrex_api_secret', None)
    SLEEP_SECONDS = 20

    # Initialize bittrex api
    api = Bittrex(API_KEY, API_SECRET, api_version=API_V1_1)

    # Initialize mongo api
    mongo = MongoClient(config.get('db', None))
    mongo.server_info()
    db = mongo.dumbot

    while True:
        ticker_cache = {}
        for position in db.positions.find({"status": "open"}):
            try:
                # Positions values
                POS_BASE_CURRENCY = position.get('market').split('-')[0]
                POS_CURRENCY = position.get('market').split('-')[1]
                POS_AMOUNT = position.get('volume')
                POS_BUY_PRICE = position.get('open_rate')

                # Init. stoppers configuration
                STOPLOSS_LIMIT = position.get('stop_loss', None)
                STOPGAIN_LIMIT = position.get('stop_profit', None)

                #balance = api.get_balance(POS_CURRENCY).get('result', {}).get('Available', 0)
                #if POS_AMOUNT > balance:
                #    print("Wallet balance (%s) mismatches the POS_AMOUNT (%s)" % (balance, POS_AMOUNT))
                #    continue

                # Get ticker value
                if "%s-%s" % (POS_BASE_CURRENCY, POS_CURRENCY) not in ticker_cache:
                    ticker_cache["%s-%s" % (POS_BASE_CURRENCY, POS_CURRENCY)] = api.get_ticker(
                        "%s-%s" % (POS_BASE_CURRENCY, POS_CURRENCY)).get('result', {}).get('Last', None)
                    if ticker_cache["%s-%s" % (POS_BASE_CURRENCY, POS_CURRENCY)] is None:
                        print("Cannot get last ticker value for %s-%s" % (POS_BASE_CURRENCY, POS_CURRENCY))
                        continue
                _LAST_TICKER_VALUE = ticker_cache["%s-%s" % (POS_BASE_CURRENCY, POS_CURRENCY)]

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
                # - STOPGAIN is always STOPGAIN_PERCENTAGE% more than current price
                if _LAST_TICKER_VALUE > POS_BUY_PRICE:
                    STOPGAIN_LIMIT = _LAST_TICKER_VALUE + (_LAST_TICKER_VALUE * STOPGAIN_PERCENTAGE / 100)

                    _sl = _LAST_TICKER_VALUE - (_LAST_TICKER_VALUE * STOPLOSS_PERCENTAGE / 100)
                    if STOPLOSS_LIMIT is None or _sl > STOPLOSS_LIMIT:
                        STOPLOSS_LIMIT = _sl
                else:
                    STOPGAIN_LIMIT = POS_BUY_PRICE + (POS_BUY_PRICE * STOPGAIN_PERCENTAGE / 100)

                    _sl = POS_BUY_PRICE - (POS_BUY_PRICE * STOPLOSS_PERCENTAGE / 100)
                    if STOPLOSS_LIMIT is None or _sl > STOPLOSS_LIMIT:
                        STOPLOSS_LIMIT = _sl

                # Recalculate the net
                expected_net = (POS_AMOUNT * _LAST_TICKER_VALUE) - (POS_AMOUNT * POS_BUY_PRICE)
                expected_net_percent = (((POS_AMOUNT * _LAST_TICKER_VALUE) * 100) / (POS_AMOUNT * POS_BUY_PRICE)) - 100
                stop_loss_percent = (((POS_AMOUNT * STOPLOSS_LIMIT) * 100) / (POS_AMOUNT * POS_BUY_PRICE)) - 100
                stop_profit_percent = (((POS_AMOUNT * STOPGAIN_LIMIT) * 100) / (POS_AMOUNT * POS_BUY_PRICE)) - 100
                db.positions.update_one({'_id': position.get('_id')}, {
                    '$set': {
                        'stop_loss_percent': stop_loss_percent,
                        'stop_profit_percent': stop_profit_percent,
                        'stop_loss': STOPLOSS_LIMIT,
                        'stop_profit': STOPGAIN_LIMIT,
                        'expected_net': expected_net,
                        'expected_net_percent': expected_net_percent,
                        'last_update_at': dt.datetime.utcnow(),
                    }})
                print(" > %s-%s Last:%s, Stop loss @%s, Stop gain @%s" % (
                    POS_BASE_CURRENCY, POS_CURRENCY, _LAST_TICKER_VALUE, STOPLOSS_LIMIT, STOPGAIN_LIMIT))

                # If limits are defined and reached then we may close positions
                closure_reason = None
                if STOPLOSS_LIMIT is not None and _LAST_TICKER_VALUE <= STOPLOSS_LIMIT:
                    closure_reason = 'stoploss'
                elif STOPGAIN_LIMIT is not None and _LAST_TICKER_VALUE >= STOPGAIN_LIMIT:
                    closure_reason = 'stopprofit'

                # Get the hell out of here, we closed the position
                if closure_reason is not None:
                    print(" > Closing position %s-%s %s@%s on %s @%s, expected_net:%s" % (
                        POS_BASE_CURRENCY, POS_CURRENCY, POS_AMOUNT, POS_BUY_PRICE,
                        closure_reason, _LAST_TICKER_VALUE, expected_net))

                    if not DRY_RUN:
                        r = api.sell_limit("%s-%s" % (POS_BASE_CURRENCY, POS_CURRENCY),
                                           quantity=POS_AMOUNT, rate=_LAST_TICKER_VALUE)
                        if not r.get('success', False):
                            raise Exception("Cannot close position: %s" % r)

                        close_order_id = r.get('result', {}).get('uuid', None)
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
                        print(" > DRY_RUN mode: position not closed.")
                    continue
            except Exception as e:
                print("Error in position handling: %s" % e)
                continue

        time.sleep(SLEEP_SECONDS)
except Exception as e:
    print("Error: %s" % e)
finally:
    print("Stopped")
