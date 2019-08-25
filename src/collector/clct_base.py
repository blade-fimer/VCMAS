#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/13 2:42 PM
# @Author  : Hao Yuan
# @E-mail  : yuanhao12@gmail.com

from gevent import monkey
monkey.patch_all()
import gevent

import os
import ssl
import json
import logging
import websocket

from utils.msg_queue import MessagingMixin
from utils.utils import *


__all__ = ["BaseCollector"]

class BaseCollector(MessagingMixin):

    def __init__(self, hostname="unknown", host=None, wss_host=None, symbols=None):
        self.ws = None
        self.hostname = hostname
        self.host = host
        self.wss_host = wss_host
        self.symbols = symbols
        MessagingMixin.__init__(self, symbols)

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
        msg = "### ERROR on {0}: {1} ###".format(self.hostname, error)
        logging.error(msg)

    def on_close(self):
        msg = "### {} closed ###".format(self.hostname)
        if not os.path.isfile(MON_FILE):
            fp = open(MON_FILE, 'w+')
            fp.close()
        logging.error(msg)

    def on_open(self):
        assert 0, "Not overrided"

    def pre_processing(self, raw_msg):
        assert 0, "Not overrided"

    def send(self, out_msg):
        try:
            if self.hostname:
                self._send(out_msg)
                # self.set(self.hostname, json.dumps(out_msg))
            else:
                logging.error("Message parsing failed for: {}".format(json.dumps(out_msg)))
        except Exception as e:
            # TODO: Handle exception
            logging.error("Send message failed:\n{}".format(e))
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

