#!/usr/bin/env python
#-*- coding:UTF-8 -*-

import logging
import time
import logging.handlers

class Logger:
    # out = '../log/'
    out = 'log/'
    # log_f = '{:*^120s}'
    logger = None
    # pauseHa = 900
    # errPid = 999999999

    def __init__(self):
        data = time.strftime("%Y%m%d-%H%M", time.localtime())
        self.out = self.out + data + '.out'
        with open(self.out, 'w+', encoding='utf-8') as f:
            f.write("")
        formatString = logging.Formatter('%(asctime)s - %(levelname)s : %(message)s')

        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)

        streamHandler = logging.StreamHandler()
        streamHandler.setFormatter(formatString)

        fileHandler = logging.FileHandler(self.out, 'a+', encoding='utf-8')
        fileHandler.setFormatter(formatString)

        self.logger.addHandler(streamHandler)
        self.logger.addHandler(fileHandler)
        self.logger.info("")
        self.logger.info("")

    def info(self, message='', s=1, **kwargs):
        message = str(message)
        if s:
            # string = self.log_f.format(string)
            message = message
        else:
            message = message
        self.logger.info(message,**kwargs)

    def warning(self, string=''):
        string = str(string)
        string = string
        self.logger.warning(string)

    def error(self, string=''):
        string = str(string)
        # string = self.log_f.format(string)
        self.logger.error(string)

    def get_file_handler(self):
        """返回一个将日志记录到文件中的处理器(FileHandler)"""
        file_handler = logging.handlers.RotatingFileHandler(
            self.out, mode='a', maxBytes=10*1024*1024, backupCount=10, encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        return file_handler

    def get_logger(self, logger_name):
        """返回一个新的logger实例（加了FileHandler）"""
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)
        logger.addHandler(self.get_file_handler())
        return logger

logger = Logger()
# logger = Logger.logger
date = Logger.out

# Example usage
logger.info("Starting performance testing...")
try:
    # some code here
    logger.info("Code executed successfully")
except Exception as e:
    logger.error("An error occurred: {}".format(str(e)))
