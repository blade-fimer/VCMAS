#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/13 2:41 PM
# @Author  : Hao Yuan
# @E-mail  : paul_yuan@sphinx.work

import json
import gzip
import logging

from clct_base import BaseCollector


__all__ = ['HuobiCollector']

class HuobiCollector(BaseCollector):

    SUB_ID = "haveAtest"

    def __init__(self,
                 hostname="Huobi",
                 host="https://api.huobi.pro",
                 wss_host="wss://api.huobi.pro/ws"):
        BaseCollector.__init__(self, hostname=hostname, host=host, wss_host=wss_host)
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
        sub = {
            "sub": HuobiCollector.DEPTH(),
            "id": HuobiCollector.SUB_ID,
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
        :param msg:
        :return:
        '''
        out = {"vals": {"huobi": {}}}
        try:
            out["vals"]["huobi"] = {}
            out["vals"]["huobi"]["bids"] = msg["tick"]["bids"][:5]   # Get top 5 depth data only
            out["vals"]["huobi"]["asks"] = msg["tick"]["asks"][:5]
            out["ts"] = msg["tick"]["ts"] // 1000
            return out
        except Exception as e:
            logging.error(e)

        return

