# Copyright 2021 Optiver Asia Pacific Pty. Ltd.
#
# This file is part of Ready Trader Go.
#
#     Ready Trader Go is free software: you can redistribute it and/or
#     modify it under the terms of the GNU Affero General Public License
#     as published by the Free Software Foundation, either version 3 of
#     the License, or (at your option) any later version.
#
#     Ready Trader Go is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU Affero General Public License for more details.
#
#     You should have received a copy of the GNU Affero General Public
#     License along with Ready Trader Go.  If not, see
#     <https://www.gnu.org/licenses/>.
import asyncio
import itertools
from time import sleep
from threading import Lock, Thread
from concurrent.futures import ThreadPoolExecutor

from typing import List

from ready_trader_go import BaseAutoTrader, Instrument, Lifespan, MAXIMUM_ASK, MINIMUM_BID, Side

# Original Config
POSITION_LIMIT = 100
UNHEDGED_LOT_LIMIT = 10
TICK_SIZE_IN_CENTS = 100
MIN_BID_NEAREST_TICK = (MINIMUM_BID + TICK_SIZE_IN_CENTS) // TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS
MAX_ASK_NEAREST_TICK = MAXIMUM_ASK // TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS
EVENT_INTERVAL = 0.05

# Custom Config
LOT_FOLLOWER_RATIO = 0.01
FUT_PREMIUM_FACTOR = 1
FUT_CAP_WEIGHT, ETF_CAP_WEIGHT, TIC_CAP_WEIGHT = 0.4, 0.3, 0.3

MARKET_EXECUTOR_CHECK_INTERVAL = 0.3
TIME_BEFORE_MARKET_ORDER = 30
CNT_TIME_BEFORE_MARKET_ORDER = TIME_BEFORE_MARKET_ORDER // EVENT_INTERVAL

class AutoTrader(BaseAutoTrader):
    """Example Auto-trader.

    When it starts this auto-trader places ten-lot bid and ask orders at the
    current best-bid and best-ask prices respectively. Thereafter, if it has
    a long position (it has bought more lots than it has sold) it reduces its
    bid and ask prices. Conversely, if it has a short position (it has sold
    more lots than it has bought) then it increases its bid and ask prices.
    """

    def __init__(self, loop: asyncio.AbstractEventLoop, team_name: str, secret: str):
        """Initialise a new instance of the AutoTrader class."""
        super().__init__(loop, team_name, secret)
        self.order_ids = itertools.count(1)
        self.bids = set()
        self.asks = set()
        self.completed_ids = set()
        self.ask_id = self.ask_price = self.bid_id = self.bid_price = self.position = 0
        self.fut_position = 0
        self.fut_ask_orders = {}
        self.fut_bid_orders = {}
        self.msg_cnt = 0
        self.order_lock = Lock()
        Thread(target=self._market_order_executor).start()
        
        self.etf_book = {Side.BID: ([],[]), Side.ASK: ([],[])}
        self.fut_book = {Side.BID: ([],[]), Side.ASK: ([],[])}
        self.etf_tick = {Side.BID: ([],[]), Side.ASK: ([],[])}
        self.fut_tick = {Side.BID: ([],[]), Side.ASK: ([],[])}

    def on_error_message(self, client_order_id: int, error_message: bytes) -> None:
        """Called when the exchange detects an error.

        If the error pertains to a particular order, then the client_order_id
        will identify that order, otherwise the client_order_id will be zero.
        """
        if client_order_id != 0 and (client_order_id in self.bids or client_order_id in self.asks):
            self.on_order_status_message(client_order_id, 0, 0, 0)

    def on_hedge_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        """Called when one of your hedge orders is filled.

        The price is the average price at which the order was (partially) filled,
        which may be better than the order's limit price. The volume is
        the number of lots filled at that price.
        """
        # Update position and order volumns
        self.order_lock.acquire()
        try:
            if client_order_id in self.fut_bid_orders:
                self.fut_position += volume
                self.fut_bid_orders[client_order_id] -= volume
                if (self.fut_bid_orders[client_order_id]==0):
                    self.fut_bid_orders.pop(client_order_id)
            elif client_order_id in self.fut_ask_orders:
                self.fut_position -= volume
                self.fut_ask_orders[client_order_id] -= volume
                if (self.fut_ask_orders[client_order_id]==0):
                    self.fut_ask_orders.pop(client_order_id)
        finally:
            self.order_lock.release()

    def _market_order_executor(self):
        """" Check current position by a preset interval and send market orders if needed """
        while True:
            if (self.msg_cnt > CNT_TIME_BEFORE_MARKET_ORDER):
                self.order_lock.acquire()
                try:
                    total = self.position + self.fut_position
                    if abs(total) > UNHEDGED_LOT_LIMIT:
                        if total < 0:
                            side, price = Side.BID, MAX_ASK_NEAREST_TICK
                            for id in self.fut_bid_orders:
                                if id in self.completed_ids:
                                    self.fut_bid_orders.pop(id)
                                elif id is not None:
                                    self.send_cancel_order(id)
                        else:
                            side, price = Side.ASK, MIN_BID_NEAREST_TICK
                            for id in self.fut_ask_orders:
                                if id in self.completed_ids:
                                    self.fut_ask_orders.pop(id)
                                elif id is not None:
                                    self.send_cancel_order(id)

                        # Sending a market order
                        sleep(0.4)
                        order_id = next(self.order_ids)
                        volume = abs(total) - (UNHEDGED_LOT_LIMIT-1)
                        self.send_hedge_order(order_id, side, price, volume)
                        if side == Side.ASK:
                            self.fut_ask_orders[order_id] = volume
                        else:
                            self.fut_bid_orders[order_id] = volume
                        self.msg_cnt = 0
                finally:
                    self.order_lock.release()
            sleep(MARKET_EXECUTOR_CHECK_INTERVAL)
            
    
    def on_order_book_update_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
                                     ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        """Called periodically to report the status of an order book.

        The sequence number can be used to detect missed or out-of-order
        messages. The five best available ask (i.e. sell) and bid (i.e. buy)
        prices are reported along with the volume available at each of those
        price levels.
        """
        self.msg_cnt += 1
        if instrument == Instrument.FUTURE:
            # Update local order book
            self.fut_book[Side.ASK] = (ask_prices, ask_volumes)
            self.fut_book[Side.BID] = (bid_prices, bid_volumes)
            return
        elif instrument == Instrument.ETF:
            # Update local order book
            self.etf_book[Side.ASK] = (ask_prices, ask_volumes)
            self.etf_book[Side.BID] = (bid_prices, bid_volumes)
            # self.execute_order_by_etf(bid_prices, ask_prices)
            

        # Estimate market inclination and set order size accordingly
        buy_total_cap, sell_total_cap = self.calculate_sides_capital()
        if sell_total_cap == 0 or sell_total_cap == 0:
            return
        buy_sell_ratio = buy_total_cap / sell_total_cap 
        is_buyer_market = buy_sell_ratio > 1.0
        is_seller_market = buy_sell_ratio < 1.0
        adjusted_ratio = LOT_FOLLOWER_RATIO * abs(buy_sell_ratio-1.0)
        
        # Cancel any current orders
        if self.bid_id != 0:
            if self.bid_id in self.completed_ids:
                self.bid_id = 0
            else:
                self.send_cancel_order(self.bid_id)
                return 

        if self.ask_id != 0:
            if self.ask_id in self.completed_ids:
                self.ask_id = 0
            else:
                self.send_cancel_order(self.ask_id) 
                return

        def _find_best_price_volumn(prices, volumes):
            return sorted(zip(prices,volumes), key=lambda x: x[1])[-1]

        self.order_lock.acquire()
        try:
            # Execute available orders
            should_buy = self.bid_id == 0 and self.position < POSITION_LIMIT
            if is_buyer_market and should_buy:
                price, volumn = _find_best_price_volumn(bid_prices, bid_volumes)
                self.bid_price = price + MIN_BID_NEAREST_TICK
                target_order_size = int(volumn * adjusted_ratio)
                order_size = min(target_order_size, abs(POSITION_LIMIT-self.position))
                if order_size == 0:
                    return
                self.bid_id = next(self.order_ids)
                self.send_insert_order(self.bid_id, Side.BUY, self.bid_price, order_size, Lifespan.GOOD_FOR_DAY)
                self.bids.add(self.bid_id)

            should_sell = self.ask_id == 0 and self.position > -POSITION_LIMIT
            if is_seller_market and should_sell:
                price, volumn = _find_best_price_volumn(ask_prices, ask_volumes)
                self.ask_price = price - MIN_BID_NEAREST_TICK
                target_order_size = int(volumn * adjusted_ratio)
                order_size = min(target_order_size, abs(-POSITION_LIMIT-self.position))
                if order_size == 0:
                    return
                self.ask_id = next(self.order_ids)
                self.send_insert_order(self.ask_id, Side.SELL, self.ask_price, order_size, Lifespan.GOOD_FOR_DAY)
                self.asks.add(self.ask_id)
        finally:
            self.order_lock.release()

    def calculate_sides_capital(self):
        fut_buy = self._get_total_capital(*self.fut_book[Side.BID])
        fut_sell = self._get_total_capital(*self.fut_book[Side.ASK])

        etf_buy = self._get_total_capital(*self.etf_book[Side.BID])
        etf_sell = self._get_total_capital(*self.etf_book[Side.ASK])
    
        fut_tic_buy = self._get_total_capital(*self.fut_tick[Side.BID])
        fut_tic_sell = self._get_total_capital(*self.fut_tick[Side.ASK])

        etf_tic_buy = self._get_total_capital(*self.etf_tick[Side.BID])
        etf_tic_sell = self._get_total_capital(*self.etf_tick[Side.ASK])

        buy_total_cap = self._get_side_total_capital(fut_buy, etf_buy, fut_tic_buy, etf_tic_buy)
        sell_total_cap = self._get_side_total_capital(fut_sell, etf_sell, fut_tic_sell, etf_tic_sell)

        return (buy_total_cap, sell_total_cap)

    def _get_side_total_capital(self, fut_cap, etf_cap, fut_tic_cap, etf_tic_cap):
        return FUT_CAP_WEIGHT*fut_cap + ETF_CAP_WEIGHT*etf_cap + TIC_CAP_WEIGHT*(fut_tic_cap+etf_tic_cap)
    
    def _get_total_capital(self, prices, vols):
        return sum(map(lambda x: x[0]*x[1], zip(prices, vols)))

    def on_order_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        """Called when one of your orders is filled, partially or fully.

        The price is the price at which the order was (partially) filled,
        which may be better than the order's limit price. The volume is
        the number of lots filled at that price.
        """
        
        self.order_lock.acquire()
        try:
            should_wait = False
            if client_order_id in self.bids:
                self.position += volume
                for id in self.fut_ask_orders.keys():
                    if id in self.completed_ids:
                        self.fut_ask_orders.pop(id)
                    else:
                        self.send_cancel_order(id)
                        should_wait = True
                if should_wait:
                    return
                if self.fut_position > -POSITION_LIMIT:
                    order_id = next(self.order_ids)
                    price += (FUT_PREMIUM_FACTOR * MIN_BID_NEAREST_TICK)
                    fut_vol = min(volume, abs(-POSITION_LIMIT - self.fut_position))
                    self.send_hedge_order(order_id, Side.ASK, price, fut_vol)
                    self.fut_ask_orders[order_id] = volume

            elif client_order_id in self.asks:
                self.position -= volume
                
                for id in self.fut_bid_orders.keys():
                    if id in self.completed_ids:
                        self.fut_bid_orders.pop(id)
                    else:
                        self.send_cancel_order(id)
                        should_wait = True
                if should_wait:
                    return
                if self.fut_position < POSITION_LIMIT:
                    order_id = next(self.order_ids)
                    price -= (FUT_PREMIUM_FACTOR * MIN_BID_NEAREST_TICK)
                    fut_vol = min(volume, abs(POSITION_LIMIT - self.fut_position))
                    self.send_hedge_order(order_id, Side.BID, price, fut_vol)
                    self.fut_bid_orders[order_id] = volume
        finally:
            self.order_lock.release()
        
    def on_order_status_message(self, client_order_id: int, fill_volume: int, remaining_volume: int,
                                fees: int) -> None:
        """Called when the status of one of your orders changes.

        The fill_volume is the number of lots already traded, remaining_volume
        is the number of lots yet to be traded and fees is the total fees for
        this order. Remember that you pay fees for being a market taker, but
        you receive fees for being a market maker, so fees can be negative.

        If an order is cancelled its remaining volume will be zero.
        """
        if remaining_volume == 0:
            self.completed_ids.add(client_order_id)
            if client_order_id == self.bid_id:
                self.bid_id = 0
            elif client_order_id == self.ask_id:
                self.ask_id = 0

            # It could be either a bid or an ask
            self.bids.discard(client_order_id)
            self.asks.discard(client_order_id)

    def on_trade_ticks_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
                               ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        """Called periodically when there is trading activity on the market.

        The five best ask (i.e. sell) and bid (i.e. buy) prices at which there
        has been trading activity are reported along with the aggregated volume
        traded at each of those price levels.

        If there are less than five prices on a side, then zeros will appear at
        the end of both the prices and volumes arrays.
        """
        if instrument == Instrument.FUTURE:
            # Update local order book
            self.fut_tick[Side.ASK] = (ask_prices, ask_volumes)
            self.fut_tick[Side.BID] = (bid_prices, bid_volumes)
            
        elif instrument == Instrument.ETF:
            # Update local order book
            self.etf_tick[Side.ASK] = (ask_prices, ask_volumes)
            self.etf_tick[Side.BID] = (bid_prices, bid_volumes)

        # self.execute_etf_order(bid_prices, ask_prices, buy_total_cap, sell_total_cap)
