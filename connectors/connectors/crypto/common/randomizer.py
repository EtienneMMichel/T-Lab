from connectors.crypto.common.logger import log
import numpy as np
import threading
import traceback
import random
import math
import time

class TradeRandomizer:

    def __init__(self, connector, symbol, side, size, trade_margin=False, time_target=300, time_limit=1200, alpha_volume=0.03, tightness=0.1):
        self.exchange_connector = connector
        self.symbol = symbol
        self.side = side
        self.amount = float(size)
        self.trade_margin = trade_margin
        self.time_target = time_target
        self.time_limit = time_limit
        self.alpha_volume = float(alpha_volume)
        self.tightness = float(tightness)

        # Parameters
        self.offername = self.side
        self.d_1 = self.tightness * self.amount
        self.alpha = self.amount / self.time_target

        # Shared variables
        self.time_start = None
        self.done = 0
        self.amount_sent = 0
        self.volume_passed = 0
        self.median_volume = 0
        self.exception_count = 0
        self.previous_order_update_timestamp = 0

        # Exchange data
        self.exchange_products = {}
        self.exchange_orderbooks = {}
        self.exchange_trades = {}

    def orderbook_callback(self, connector, data):
        symbol = data["symbol"]
        self.exchange_orderbooks[symbol] = data

    def orders_callback(self, connector, data):
        print("---------------------------")
        print("--- ORDERS CALLBACK")
        print("---------------------------")
        print(data)
        print("---------------------------")

    def trades_callback(self, connector, data):
        self.volume_passed += data["amount"]

    def initiate_tasks(self):
        self.exchange_products = self.exchange_connector.get_products()

        #TODO: Refactor this part when get_orderbook output format has been updated
        ###
        orderbook = self.exchange_connector.get_orderbook(self.symbol, depth=5)
        refactored_orderbook = {
            "buy": {},
            "sell": {},
        }

        for order in orderbook["buy"]:
            refactored_orderbook["buy"][order["price"]] = order["size"]
        for order in orderbook["sell"]:
            refactored_orderbook["sell"][order["price"]] = order["size"]
        ###

        self.exchange_orderbooks[self.symbol] = {
            "symbol": self.symbol,
            "buy": dict(sorted(refactored_orderbook["buy"].items(), reverse=True)),
            "sell": dict(sorted(refactored_orderbook["sell"].items(), reverse=False)),
        }

        self.exchange_trades[self.symbol] = []

        #TODO: Temporarilly enable websocket logs
        self.exchange_connector.show_websocket_logs(False)

        #self.exchange_connector.subscribe_orders(self.symbol, self.orders_callback)
        self.exchange_connector.subscribe_orderbook(self.symbol, self.orderbook_callback, depth=5)
        self.exchange_connector.subscribe_trades(self.symbol, self.trades_callback)

        self.exchange_trades = self.exchange_connector.get_public_trades(self.symbol)
        self.median_volume = np.median([trade['size'] for trade in self.exchange_trades])

        product = self.exchange_products[self.symbol]
        self.contract_size = product["contract_size"]
        self.min_size = self.get_min_size()

        self.msleep(1000)

    def cleanup(self):
        log(f"Cleanup websockets")
        #self.exchange_connector.unsubscribe_orders(self.symbol, self.orders_callback)
        log(f"Unsubscribe orderbook channel")
        self.exchange_connector.unsubscribe_orderbook(self.symbol, self.orders_callback)
        log(f"Unsubscribe trades channel")
        self.exchange_connector.unsubscribe_trades(self.symbol, self.trades_callback)
        log(f"Cleanup connector internal threads")
        self.exchange_connector.cleanup()
        log(f"Everything cleaned up")

    def run(self):
        self.time_start = time.time()
        log(f"Start execution on {self.symbol} at {self.time_start}")

        # Wait random time
        self.msleep(5000 * random.random())

        self.initiate_tasks()
        while not self.is_finished():
            try:
                if self.catchup_needed():
                    amount = 2* random.random() * min(self.median_volume, self.d_1 / 2)
                    #log(f"### PLACE '{self.side}' ORDER OF {amount} ON {self.symbol}")
                    self.place_market_order(amount)
                    self.msleep(10 + 50*random.random())
                else:
                    log(f'Waiting for volume or time: done:{self.done}, upper_bound: {self.get_ideal_amount()}')
                    self.msleep(1000)
            except Exception as e:
                if self.exception_count > 3:
                    log("More than 3 exceptions in vwap, restarting connection.")
                else:
                    log("Exception in vwap, restarting connection.")
                log(traceback.format_exc())
                self.msleep(1000)
                log("Slept for 1s.")

                self.cleanup()
                self.initiate_tasks()
            self.msleep(100)
        log(f"Finished vwap on {self.symbol},order:{self.amount}, done: {self.done}")
        log(f"Cancelling remaining orders on {self.symbol}.")
        self.cleanup()
        self.msleep(1000)
        self.done = self.get_amount_done()
        log(f"Exit randomizer")
        return

    def msleep(self, milliseconds):
        time.sleep(milliseconds/1000)

    def get_amount_done(self):
        return self.amount_sent

    def get_min_size(self):
        appr_price = self.get_best_offer()
        contract_size = self.contract_size
        ten_dollars_order = 11 / (appr_price * contract_size)
        tick_size = self.exchange_products[self.symbol]["limits"]["amount"]["min"]
        min_size = max(tick_size, ten_dollars_order)
        return min_size

    def get_best_offer(self):
        first_key = list(self.exchange_orderbooks[self.symbol][self.offername].keys())[0]
        return first_key

    def place_market_order(self, amount, checks = True):
        try:
            if checks:
                amount = max(0,min(amount, self.amount - self.done))
            amount_rounded = self.round_amount(amount)
            if amount > 0:
                log(f"### PLACE '{self.side}' ORDER OF {amount_rounded} ON {self.symbol}")

                if self.trade_margin:
                    order = self.exchange_connector.margin_place_market_order(self.symbol, amount_rounded, self.side)
                else:
                    order = self.exchange_connector.place_market_order(self.symbol, amount_rounded, self.side)
                #self.amount_sent = self.amount_sent + amount_rounded
                self.amount_sent += order["filled_size"]
                return order
            else:
                log(f"Amount {amount} market order on {self.symbol} was rounded to {amount_rounded}.")
        except Exception as e:
            if (self.exception_count > 3):
                log("[ERROR] Exception while placing order (more than 3 exceptions total).")
            else:
                log("Exception while placing order.")
            log(traceback.format_exc())
            return None

    def round_amount(self, amount):
        return math.ceil(amount / self.min_size) * self.min_size

    def is_finished(self):
        self.done = self.get_amount_done()
        is_complete = self.amount - self.done < self.min_size
        time_remaining = self.time_limit - (time.time() - self.time_start)
        is_timeout = (time_remaining <= 0)
        is_finished = (is_complete
                    or is_timeout)
        log(f"is_finished:{is_finished}, amount: {self.amount}, done: {self.done}, time_remaining:{time_remaining}")
        return is_finished

    def catchup_needed(self):
        ideal_amount = self.get_ideal_amount()
        log(f"Ideal amount done: {ideal_amount}, done: {self.done}, min_size: {self.min_size}")
        return (self.done + self.min_size < ideal_amount)

    def get_ideal_amount(self):
        volume = self.get_volume_passed()
        amount_time = self.alpha*(time.time() - self.time_start)
        amount_volume = self.alpha_volume*volume
        log(f"Ideal amount based on time: {amount_time}, based on volume: {amount_volume}")
        return min(amount_time,amount_volume)

    def get_volume_passed(self):
        return self.volume_passed

    def get_upper_bound_amount(self):
        return self.get_ideal_amount() + self.d_1

    def get_lower_bound_amount(self):
        return self.get_ideal_amount() - self.d_1