#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/5/28 11:02 PM
# @Author  : Hao Yuan
# @E-mail  : yuanhao12@gmail.com

from gevent import monkey
monkey.patch_all()
import gevent


import ssl
import json
import gzip
import zlib
import time
import redis
import logging
import websocket
from threading import Thread

LOG_FILE = "collector.log"
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %Y/%m/%d %H:%M:%S',
                    filename=LOG_FILE,
                    filemode='a')


class MsgQueue(object):
    EXPIRE_TIME = 60

    def __init__(self):
        # Thread.__init__(self)

        # TODO: All object share one connection pool
        self._rpool = redis.ConnectionPool(host="127.0.0.1", port="6379")
        self._queue = redis.Redis(connection_pool=self._rpool)
        self._pipeline = self._queue.pipeline()

    def add(self, key, value):
        self._pipeline.rpush(key, value).expire(key, MsgQueue.EXPIRE_TIME).execute()

    def delete(self, key):
        self._queue.delete(key)


class BaseCollector(object):

    def __init__(self, hostname=None, host=None, wss_host=None):
        self.ws = None
        self.hostname = hostname
        self.host = host
        self.wss_host = wss_host
        self.queue = MsgQueue()

    @staticmethod
    def DEPTH(symbol="", depth=""):
        '''
        Override this function
        :param symbol:
        :return:
        '''
        assert 0, "Not overrided"

    @staticmethod
    def TRADE_DETAIL(symbol=""):
        '''
        Override this function
        :param symbol:
        :return:
        '''
        assert 0, "Not overrided"

    @staticmethod
    def KLINE_1min(symbol=""):
        '''
        Override this function
        :param symbol:
        :return:
        '''
        assert 0, "Not overrided"

    @staticmethod
    def KLINE_1day(symbol=""):
        '''
        Override this function
        :param symbol:
        :return:
        '''
        assert 0, "Not overrided"

    def on_message(self, raw_msg):
        '''
        Optional to override this function
        :param message:
        :return:
        '''
        g = gevent.spawn(self.pre_processing, raw_msg)
        # g.join()

    def on_error(self, error):
        logging.error(error)

    def on_close(self):
        msg = "### {} closed ###".format(self.hostname)
        print(msg)
        logging.info(msg)

    def on_open(self):
        assert 0, "Not overrided"

    def pre_processing(self, raw_msg):
        assert 0, "Not overrided"

    def send(self, out_msg):
        try:
            key = out_msg.get("ts", "")
            if key:
                self.queue.add(key, json.dumps(out_msg.get("vals", {})))
            else:
                logging.error("Message parsing failed for: {}".format(json.dumps(out_msg)))
        except Exception as e:
            # TODO: Handle exception
            pass

    def run(self):
        websocket.enableTrace(True)
        self.ws = websocket.WebSocketApp(url=self.wss_host,
                                         on_message=self.on_message,
                                         on_open=self.on_open,
                                         # on_ping=self.on_ping,
                                         on_error=self.on_error,
                                         on_close=self.on_close)
        self.ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE},)


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
        self.send(out_msg)

    def __normalize_data(self, msg):
        '''
        Target format:
        {
            "vals": {
                "huobi": {
                    "bids": [[296.42,1.6141],[296.41,5.6102],[296.4,1.3836],[296.34,9.4443],[296.32,1.35]],
                    "asks": [[296.45,1.95],[296.5,15.2889],[296.55,5.0],[296.56,1.5182],[296.57,1.5182]],
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


class OkexCollector(BaseCollector):
    EXPIRE_TIME = 12

    def __init__(self,
                 hostname="Okex",
                 host="https://www.okex.com/",
                 wss_host="wss://real.okex.com:10442/ws/v3"):
        BaseCollector.__init__(self, hostname=hostname, host=host, wss_host=wss_host)
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
        sub = {
            "op": "subscribe",
            "args": [OkexCollector.DEPTH()],
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
        self.send(out_msg)

    def __inflate(self, data):
        decompress = zlib.decompressobj(
            -zlib.MAX_WBITS
        )
        inflated = decompress.decompress(data)
        inflated += decompress.flush()
        return inflated

    def __normalize_data(self, msg):
        out = {"vals": {"okex": {}}}
        try:
            out["vals"]["okex"] = {}
            # Example: "timestamp":"2019-04-16T11:03:03.712Z"
            # Ignore the timezone flag Z since it's utc time already
            out["ts"] = self.__normalize_ts(msg["data"][0]["timestamp"][:-1])
            out["vals"]["okex"]["bids"] = list(map(lambda x:x[:2], msg["data"][0]["bids"]))   # Get top 5 depth data only
            out["vals"]["okex"]["asks"] = list(map(lambda x:x[:2], msg["data"][0]["asks"]))
            return out
        except Exception as e:
            logging.error(e)

        return

    def __normalize_ts(self, stime):
        return int(time.mktime(time.strptime(stime, "%Y-%m-%dT%H:%M:%S.%f")))

def main():
    collectors = [HuobiCollector(), OkexCollector()]
    gevent.joinall([gevent.spawn(collector.run) for collector in collectors])

if __name__ == "__main__":
    main()