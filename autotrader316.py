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
from concurrent.futures import ThreadPoolExecutor

from typing import List

from ready_trader_go import BaseAutoTrader, Instrument, Lifespan, MAXIMUM_ASK, MINIMUM_BID, Side

# Original Config
POSITION_LIMIT = 100
TICK_SIZE_IN_CENTS = 100
MIN_BID_NEAREST_TICK = (MINIMUM_BID + TICK_SIZE_IN_CENTS) // TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS
MAX_ASK_NEAREST_TICK = MAXIMUM_ASK // TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS

# Custom Config
NUM_THREADS = 5
LOT_SIZE = 10
# FUT_CAP_WEIGHT, ETF_CAP_WEIGHT, TIC_CAP_WEIGHT = 0.2, 0.3, 0.5
FUT_CAP_WEIGHT, ETF_CAP_WEIGHT, TIC_CAP_WEIGHT = 0.2, 0.3, 0.5

trade_margin = -0.05
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
        self.thread_pool = ThreadPoolExecutor(max_workers=NUM_THREADS)
        self.order_ids = itertools.count(1)
        self.bids = set()
        self.asks = set()
        self.ask_id = self.ask_price = self.bid_id = self.bid_price = self.position = 0
        
        self.etf_book = {Side.BID: ([],[]), Side.ASK: ([],[])}
        self.fut_book = {Side.BID: ([],[]), Side.ASK: ([],[])}
        self.etf_tick = {Side.BID: ([],[]), Side.ASK: ([],[])}
        self.fut_tick = {Side.BID: ([],[]), Side.ASK: ([],[])}


        self.futs = list()
        self.round_count = 0
        self.fut_position = 0
        self.fut_bids = set()
        self.fut_asks = set()
    def on_error_message(self, client_order_id: int, error_message: bytes) -> None:
        """Called when the exchange detects an error.

        If the error pertains to a particular order, then the client_order_id
        will identify that order, otherwise the client_order_id will be zero.
        """
        self.logger.warning(f"error with order {client_order_id}: {error_message.decode()}")
        if client_order_id != 0 and (client_order_id in self.bids or client_order_id in self.asks):
            self.on_order_status_message(client_order_id, 0, 0, 0)

    def on_hedge_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        """Called when one of your hedge orders is filled.

        The price is the average price at which the order was (partially) filled,
        which may be better than the order's limit price. The volume is
        the number of lots filled at that price.
        """
        self.logger.info(f"received hedge filled for order({client_order_id}) with average price {price} and volume {volume}")
        if client_order_id in self.fut_bids:
            self.fut_position+=volume
        if client_order_id in self.fut_asks:
            self.fut_position-=volume        


    def on_order_book_update_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
                                     ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        """Called periodically to report the status of an order book.

        The sequence number can be used to detect missed or out-of-order
        messages. The five best available ask (i.e. sell) and bid (i.e. buy)
        prices are reported along with the volume available at each of those
        price levels.
        """
        self.logger.info(f"received order book for instrument {instrument} with sequence number {sequence_number}")

        if instrument == Instrument.FUTURE:
            
            if self.position == -self.fut_position:
                self.round_count = 0            
            if self.round_count != 0:
                print(self.round_count)
                self.round_count+=1
            if self.round_count ==1:
                net_diff = self.position + self.fut_position
                order_id = next(self.order_ids)
                if net_diff > 0:
                    self.send_hedge_order(order_id, Side.ASK, MIN_BID_NEAREST_TICK, net_diff)
                    self.fut_asks.add(order_id)
                    self.logger.info(f"sending future sell order({order_id}) of price {MIN_BID_NEAREST_TICK} and size {net_diff}")

                elif net_diff <0:
                    self.send_hedge_order(order_id, Side.BID, MAX_ASK_NEAREST_TICK, -net_diff)
                    self.fut_bids.add(order_id)
                    self.logger.info(f"sending future buy order({order_id}) of price {MAX_ASK_NEAREST_TICK} and size {-net_diff}")
            elif self.round_count < 1:
                new_price = 0
             # Update local order book
                self.fut_book[Side.ASK] = (ask_prices, ask_volumes)
                self.fut_book[Side.BID] = (bid_prices, bid_volumes)
                # self.execute_order_by_fut(bid_prices, ask_prices)
                current_order_size= 0
                etf_avg_ask = self._get_avg_price(*self.etf_book[Side.ASK])
                etf_avg_bid = self._get_avg_price(*self.etf_book[Side.BID]) 
                fut_avg_ask = self._get_avg_price(*self.fut_book[Side.ASK]) 
                fut_avg_bid = self._get_avg_price(*self.fut_book[Side.BID])
                price_adjustment = - (self.position // LOT_SIZE) * TICK_SIZE_IN_CENTS

                if etf_avg_ask  * (1+trade_margin) < fut_avg_bid:
                    new_price = int(etf_avg_ask* (1+trade_margin))
                    new_price+=price_adjustment
                    current_order_size += LOT_SIZE
                elif fut_avg_ask  < etf_avg_bid * (1-trade_margin):
                    new_price = int(etf_avg_bid* (1-trade_margin))
                    new_price+=price_adjustment
                    current_order_size -= LOT_SIZE
                
            
                    

                
                

                # Cancel any current orders if needed
                should_cancel_bid = self.bid_id != 0 and new_price not in (self.bid_price, 0)
                if should_cancel_bid:
                    self.send_cancel_order(self.ask_id) # cancel the reverse order 
                    self.send_cancel_order(self.bid_id) # cancel the current order 
                    self.bid_id = 0
                should_cancel_ask = self.ask_id != 0 and new_price not in (self.ask_price, 0)
                if should_cancel_ask:
                    self.send_cancel_order(self.bid_id) # cancel the reverse order 
                    self.send_cancel_order(self.ask_id) # cancel the current order 
                    self.ask_id = 0




                #Group both strategy and trade the net total
                if new_price>0:
                    if current_order_size >0:
                        self.bid_id = next(self.order_ids)
                        self.bid_price = new_price
                        order_size = min(current_order_size, abs(POSITION_LIMIT-self.position))
                        self.send_insert_order(self.bid_id, Side.BUY, new_price, order_size, Lifespan.GOOD_FOR_DAY)
                        self.bids.add(self.bid_id)
                        self.logger.info(f"sending buy order({self.bid_id}) at {new_price} of size {order_size}")
                    if current_order_size < 0:
                        self.ask_id = next(self.order_ids)
                        self.ask_price = new_price
                        order_size = min(abs(current_order_size), abs(-POSITION_LIMIT-self.position))
                        self.send_insert_order(self.ask_id, Side.SELL, new_price, order_size, Lifespan.GOOD_FOR_DAY)
                        self.asks.add(self.ask_id)
                        self.logger.info(f"sending sell order({self.ask_id}) at {new_price} of size {order_size}")

            
        elif instrument == Instrument.ETF:
            # Update local order book
            self.etf_book[Side.ASK] = (ask_prices, ask_volumes)
            self.etf_book[Side.BID] = (bid_prices, bid_volumes)
            # self.execute_order_by_etf(bid_prices, ask_prices)
        

    def on_order_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        """Called when one of your orders is filled, partially or fully.

        The price is the price at which the order was (partially) filled,
        which may be better than the order's limit price. The volume is
        the number of lots filled at that price.
        """
        self.logger.info(f"received order({client_order_id} filled of price {price} and volume {volume}")
        if self.round_count !=0:
            if self.position == -self.fut_position:
                self.round_count = 0
        else:
            if self.position != -self.fut_position:
                self.round_count = 1
        if client_order_id in self.bids:
            self.position += volume
            order_id = next(self.order_ids)
            self.send_hedge_order(order_id, Side.ASK, int(price*(1+trade_margin)), volume)
            self.logger.info(f"sending future sell order({order_id}) of price {price*(1+trade_margin)} and size {volume}")
        elif client_order_id in self.asks:
            self.position -= volume
            order_id = next(self.order_ids)
            self.send_hedge_order(order_id, Side.BID, int(price*(1-trade_margin)), volume)
            self.logger.info(f"sending future buy order({order_id}) of price {price*(1-trade_margin)} and size {volume}")          

   




    
    def on_order_status_message(self, client_order_id: int, fill_volume: int, remaining_volume: int,
                                fees: int) -> None:
        """Called when the status of one of your orders changes.

        The fill_volume is the number of lots already traded, remaining_volume
        is the number of lots yet to be traded and fees is the total fees for
        this order. Remember that you pay fees for being a market taker, but
        you receive fees for being a market maker, so fees can be negative.

        If an order is cancelled its remaining volume will be zero.
        """
        self.logger.info("received order status for order %d with fill volume %d remaining %d and fees %d",
                         client_order_id, fill_volume, remaining_volume, fees)
        if remaining_volume == 0:
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
        self.logger.info("received trade ticks for instrument %d with sequence number %d", instrument,
                         sequence_number)
    def _get_avg_price(self,prices,vols):
        new_vol = [x if x<LOT_SIZE else LOT_SIZE for x in vols]
        v_sum = sum(new_vol)
        if v_sum== 0:
            return 0
        tot = 0
        for i in range(len(prices)):
            tot+= new_vol[i]/v_sum * prices[i]
        return tot