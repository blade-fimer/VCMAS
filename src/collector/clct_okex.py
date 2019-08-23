#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/13 2:45 PM
# @Author  : Hao Yuan
# @E-mail  : yuanhao12@gmail.com

import json
import time
import zlib
import logging
from threading import Thread

from clct_base import BaseCollector
from utils.msg_queue import MessagingMixin


__all__ = ['OkexCollector']

class OkexCollector(BaseCollector):
    EXPIRE_TIME = 12
    CH_CUSTOMIZE_MAPPING = {
        "ethusdt": "ETH-USDT",
    }

    CH_NORMALIZE_MAPPING = {
        "ETH-USDT": "ethusdt",
    }

    def __init__(self,
                 hostname="okex",
                 host="https://www.okex.com/",
                 wss_host="wss://real.okex.com:10442/ws/v3",
                 symbols=["ethusdt"]):
        BaseCollector.__init__(self, hostname=hostname, host=host, wss_host=wss_host, symbols=symbols)
        self.ping_thread = None
        self._reset_expire_time()

    @staticmethod
    def DEPTH(symbol="ETH-USDT", depth="5"):
        # Okex provides only 5 entries or 200 entries of depth data
        if depth != "5":
            depth = ""
        return "spot/depth{0}:{1}".format(depth, symbol)

    @staticmethod
    def TRADE_DETAIL(symbol="ETH-USDT"):
        return "spot/trade:{}".format(symbol)

    @staticmethod
    def KLINE_1min(symbol="ETH-USDT"):
        return "spot/candle60s:{}".format(symbol)

    @staticmethod
    def KLINE_1day(symbol="ETH-USDT"):
        return "spot/candle86400s:{}".format(symbol)

    def _reset_expire_time(self):
        self.expire_time = OkexCollector.EXPIRE_TIME

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
        for symbol in self.symbols:
            sub = {
                "op": "subscribe",
                "args": [OkexCollector.DEPTH(OkexCollector.CH_CUSTOMIZE_MAPPING[symbol])],
            }
            self.ws.send(json.dumps(sub))

        self.ping_thread = Thread(target=self._ping)
        self.ping_thread.start()

    def pre_processing(self, raw_msg):
        # Reset the timer to send ping message in case new message is received
        self._reset_expire_time()
        out_msg = self.__inflate(raw_msg).decode("utf-8")
        logging.debug("Get message:\n{0}".format(out_msg))
        out_msg = json.loads(out_msg)
        if out_msg.get("ping"):
            # self._on_ping(out_msg)
            out_msg = ""
            return

        out_msg = self.__normalize_data(out_msg)
        logging.debug("Normalized message:\n{0}".format(out_msg))
        self.send(out_msg)

    def __inflate(self, data):
        decompress = zlib.decompressobj(
            -zlib.MAX_WBITS
        )
        inflated = decompress.decompress(data)
        inflated += decompress.flush()
        return inflated

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
            out["ch"] = OkexCollector.CH_NORMALIZE_MAPPING[msg["data"][0]["instrument_id"]]
            out["host"] = MessagingMixin.HOST_ID[self.hostname]
            out["ts"] = self.__normalize_ts(msg["data"][0]["timestamp"][:-1])
            out["bids"] = list(map(lambda x:x[:2], msg["data"][0]["bids"]))   # Get top 5 depth data only
            out["asks"] = list(map(lambda x:x[:2], msg["data"][0]["asks"]))
            return out
        except Exception as e:
            logging.error(e)

        return

    def __normalize_ts(self, stime):
        return int(time.mktime(time.strptime(stime, "%Y-%m-%dT%H:%M:%S.%f")))