#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/13 3:34 PM
# @Author  : Hao Yuan
# @E-mail  : yuanhao12@gmail.com


from msg_queue import MsgQueue

class Pig():
    def __init__(self):
        self._queue = MsgQueue()

    def chew(self):
        self.