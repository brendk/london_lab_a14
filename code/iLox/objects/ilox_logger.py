# -*- coding: utf-8 -*-
"""
Created on Thu Jun  3 01:03:26 2021

@author: brend
"""

import sys
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime




# Redirect all errors to log file instead of sys.stderr
class StreamToLogger(object):
    
    def __init__(self, logger, level):
       self.logger = logger
       self.level = level
       self.linebuf = ''

    def write(self, buf):
       for line in buf.rstrip().splitlines():
          self.logger.log(self.level, line.rstrip())

    def flush(self):
        pass


class iLoxLogger():
    
    def __init__(self, log_params):
        
        self.log_params = log_params
            
        # File handler for log file
        handler_file = TimedRotatingFileHandler(
            filename = self.log_params["log_file"] % datetime.utcnow().strftime("%Y-%m-%d"), 
            when = "d", interval = 1, backupCount = 10)
        
        logging_params = {'level': logging.INFO, 
                          'format': '%(asctime)s %(message)s', 
                          'datefmt': '%Y/%m/%d %H:%M:%S', 
                          'handlers': [handler_file],}
        logging.basicConfig(**logging_params)
        self.logger = logging.getLogger("iLox")
        
        if self.log_params["redirect_stdout"]:
            # Redirect stdout to logging
            sys.stdout = StreamToLogger(self.logger, logging.INFO)
        if self.log_params["redirect_stderr"]:
            # Redirect stderr to logging
            sys.stderr = StreamToLogger(self.logger, logging.INFO)
            
    def info(self, logtext):
        self.logger.info(logtext)
        
    def debug(self, logtext):
        self.logger.debug(logtext)
        
    def error(self, logtext):
        self.logger.error(logtext)
            
    def display_pb(self):
        return not self.log_params["show_pb"]
    