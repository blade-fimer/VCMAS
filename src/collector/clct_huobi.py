#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/13 2:41 PM
# @Author  : Hao Yuan
# @E-mail  : yuanhao12@gmail.com

import json
import gzip
import logging

from clct_base import BaseCollector
from utils.msg_queue import MessagingMixin


__all__ = ['HuobiCollector']

class HuobiCollector(BaseCollector):
    def __init__(self,
                 hostname="huobi",
                 host="https://api.huobi.pro",
                 wss_host="wss://api.huobi.pro/ws",
                 symbols=["ethusdt"]):
        BaseCollector.__init__(self, hostname=hostname, host=host, wss_host=wss_host, symbols=symbols)
        # wss_hostx is the wss host for trading
        self.wss_hostx = "wss://api.huobi.pro/ws/v1"

    @staticmethod
    def DEPTH(symbol="ethusdt", depth="0"):
        return "market.{0}.depth.step{1}".format(symbol, depth)

    @staticmethod
    def TRADE_DETAIL(symbol="ethusdt"):
        return "market.{}.trade.detail".format(symbol)

    @staticmethod
    def KLINE_1min(symbol="ethusdt"):
        return "market.{}.kline.1min".format(symbol)

    @staticmethod
    def KLINE_1day(symbol="ethusdt"):
        return "market.{}.kline.1day".format(symbol)

    def __on_ping(self, ping):
        logging.debug("### Get ping ###")
        cont = ping.get("ping")
        self.ws.send(json.dumps({"pong": cont}))

    def on_open(self):
        for symbol in self.symbols:
            sub = {
                "sub": HuobiCollector.DEPTH(symbol),
                "id": "{0}_{1}".format(self.hostname, symbol),
            }
            logging.debug("{}".format(json.dumps(sub)))
            self.ws.send(json.dumps(sub))

    def pre_processing(self, raw_msg):
        out_msg = gzip.decompress(raw_msg).decode("utf-8")
        logging.debug("Get message:\n{0}".format(out_msg))
        out_msg = json.loads(out_msg)
        if out_msg.get("ping"):
            self.__on_ping(out_msg)
            out_msg = ""
            return
        elif out_msg.get("status"):
            logging.debug(out_msg)
            return
        else:
            out_msg = self.__normalize_data(out_msg)
            logging.debug("Normalized message:\n{0}".format(out_msg))
        self.send(out_msg)

    def __normalize_data(self, msg):
        '''
        Original format:
        {
            "ch":"market.ethusdt.depth.step0",
            "ts":1565157315511,
            "tick":{
                "bids":[[227.12,15.9999],[227.11,0.0335]...],"asks":[[227.21,6.6983],[227.22,6.199]...[229.22,0.08]],
                "version":101855031086,
                "ts":1565157315070
            }
        }

        Target format:
        {
            "vals": {
                "huobi": {
                    "bids": [["296.42","1.6141"],["296.41","5.6102"],["296.4","1.3836"],["296.34","9.4443"],["296.32","1.35"]],
                    "asks": [["296.45","1.95"],["296.5","15.2889"],["296.55","5.0"],["296.56","1.5182"],["296.57","1.5182"]],
                 },
            },
            "ts": 1559524723012 / 1000,
        }

        Since 2019-07-16, target format change to:
        {
            "ch": "market.ethusdt.depth.step0",
            "host": 0,
            "ts": 1559524723012 / 1000,
            "bids": [["296.42","1.6141"],["296.41","5.6102"],["296.4","1.3836"],["296.34","9.4443"],["296.32","1.35"]],
            "asks": [["296.45","1.95"],["296.5","15.2889"],["296.55","5.0"],["296.56","1.5182"],["296.57","1.5182"]],
        }

        :param msg:
        :return:
        '''
        out = {}
        try:
            out["ch"] = msg["ch"].split('.')[1]
            out["host"] = MessagingMixin.HOST_ID[self.hostname]
            out["ts"] = msg["tick"]["ts"] // 1000
            out["bids"] = msg["tick"]["bids"][:5]   # Get top 5 depth data only
            out["asks"] = msg["tick"]["asks"][:5]
            return out
        except Exception as e:
            logging.error(e)

        return

