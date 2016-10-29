# -*- coding:utf-8 -*-  
'''
Created on 2016年10月1日

@author: MQ
'''
import logging

class snailLogger():
    def __init__(self,url):
        # 创建一个logger
        self.logger = logging.getLogger('[snailLogger]')
        self.logger.setLevel(logging.INFO)
        # 创建一个handler，用于写入日志文件
        fh = logging.FileHandler(url)
        fh.setLevel(logging.INFO)
        # 再创建一个handler，用于输出到控制台
#         ch = logging.StreamHandler()
#         ch.setLevel(logging.DEBUG)
        # 定义handler的输出格式
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
#         ch.setFormatter(formatter)
        # 给logger添加handler
        self.logger.addHandler(fh)
#         self.logger.addHandler(ch)
    
    def get_logger(self):
        return self.logger