#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/13 3:34 PM
# @Author  : Hao Yuan
# @E-mail  : paul_yuan@sphinx.work


from msg_queue import MsgQueue

class Pig():
    def __init__(self):
        self._queue = MsgQueue()

    def chew(self):
        self.