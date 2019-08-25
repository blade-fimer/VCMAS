#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/8/25 10:54 AM
# @Author  : Hao Yuan
# @E-mail  : yuanhao12@gmail.com


import json
import time
import zlib
import logging
from threading import Thread

from clct_base import BaseCollector
from utils.msg_queue import MessagingMixin


__all__ = ['UpbitCollector']

class UpbitCollector(BaseCollector):
    # Official timeout is 120s, 50s to ensure 2 ping message sent before timeout
    EXPIRE_TIME = 50
    CH_CUSTOMIZE_MAPPING = {
        "ethusdt": "USDT-ETH",
        "btcusdt": "USDT-BTC",
    }

    CH_NORMALIZE_MAPPING = {
        "USDT-ETH": "ethusdt",
        "USDT-BTC": "btcusdt",
    }

    def __init__(self,
                 hostname="upbit",
                 host="https://www.upbit.com/",
                 wss_host="wss://api.upbit.com/websocket/v1",
                 symbols=["ethusdt"]):
        BaseCollector.__init__(self, hostname=hostname, host=host, wss_host=wss_host, symbols=symbols)
        self.ping_thread = None
        self._reset_expire_time()

    @staticmethod
    def DEPTH(symbols=["ethusdt"], depth="5"):
        # map in python3 returns a map object while python2.x returns list alternativly
        codes = list(map(lambda x: UpbitCollector.CH_CUSTOMIZE_MAPPING[x], symbols))
        return {"type": "orderbook", "codes": codes}

    @staticmethod
    def TRADE_DETAIL(symbol="USDT-ETH"):
        return "spot/trade:{}".format(symbol)

    @staticmethod
    def KLINE_1min(symbol="USDT-ETH"):
        return "spot/candle60s:{}".format(symbol)

    @staticmethod
    def KLINE_1day(symbol="USDT-ETH"):
        return "spot/candle86400s:{}".format(symbol)

    def _reset_expire_time(self):
        self.expire_time = UpbitCollector.EXPIRE_TIME

    def _ping(self):
        logging.debug("### Send ping ###")
        while True:
            if self.expire_time <= 0:
                self.ws.send("ping")
                self._reset_expire_time()
            else:
                self.expire_time -= 1
                time.sleep(1)

    def on_open(self):
        # Able to subscribe to several channels
        sub = [{"ticket": self.hostname},
               UpbitCollector.DEPTH(self.symbols)]
        self.ws.send(json.dumps(sub))

        self.ping_thread = Thread(target=self._ping)
        self.ping_thread.start()

    def pre_processing(self, raw_msg):
        # Reset the timer to send ping message in case new message is received
        self._reset_expire_time()
        # out_msg = self.__inflate(raw_msg).decode("utf-8")
        out_msg = raw_msg
        logging.debug("Get message:\n{0}".format(out_msg))
        out_msg = json.loads(out_msg)
        if out_msg.get("ping"):
            # self._on_ping(out_msg)
            out_msg = ""
            return

        out_msg = self.__normalize_data(out_msg)
        logging.debug("Normalized message:\n{0}".format(out_msg))
        self.send(out_msg)

    def __normalize_data(self, msg):
        '''
        Since 2019-07-16, format changed(refer to clct_huobi.py)
        :param msg:
        :return:
        '''
        out = {}
        try:
            # Example: "timestamp":"2019-04-16T11:03:03.712Z"
            # Ignore the timezone flag Z since it's utc time already
            out["ch"] = UpbitCollector.CH_NORMALIZE_MAPPING[msg["code"]]
            out["host"] = MessagingMixin.HOST_ID[self.hostname]

            # !!!Caution, upbit returned timestamp is not consistently updated
            out["ts"] = msg["timestamp"] // 1000
            order_book = msg["orderbook_units"]
            out["bids"] = []
            out["asks"] = []
            for m in range(0, 5):
                out["bids"].append([order_book[m]["bid_price"], order_book[m]["bid_size"]])
                out["asks"].append([order_book[m]["ask_price"], order_book[m]["ask_size"]])
            return out
        except Exception as e:
            logging.error(e)

        return
