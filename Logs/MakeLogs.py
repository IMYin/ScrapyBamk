#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Created on 12/15/16 7:54 AM

@author: imyin

@File: Logs
"""
import os
import datetime
import sys
from JobLogging import JobLogging

CONSTANTS_PATH = os.path.dirname(os.getcwd())
sys.path.append(CONSTANTS_PATH)
import Constants as cons


class Logs:
    def __init__(self, log_lev='INFO', task_name=None):
        date_today = datetime.datetime.now().date()
        # self.log_name = os.path.splitext(os.path.split(sys.argv[0])[1])[0]
        log_dir = cons.TASK_LOG_PATH
        self.today = date_today.strftime("%Y%m%d")
        log_dir += '/' + self.today
        if not os.path.isdir(log_dir):
            try:
                os.makedirs(log_dir)
            except:
                pass
        mylog = JobLogging(task_name, log_dir)
        mylog.set_level(log_lev)
        self.log = mylog.get_logger()
        self.log.info(task_name + u"'s log create success.")
