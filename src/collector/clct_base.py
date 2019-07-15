#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/13 2:42 PM
# @Author  : Hao Yuan
# @E-mail  : paul_yuan@sphinx.work

from gevent import monkey
monkey.patch_all()
import gevent

import ssl
import json
import logging
import websocket

from utils.msg_queue import MsgQueue


__all__ = ["BaseCollector"]

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
            logging.error("Send message failed:\n{}".format(e.message))
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
        logging.info("{} started".format(self.__class__.__name__))

