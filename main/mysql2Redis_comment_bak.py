# -*- coding:utf-8 -*-
'''
Created on 2016年10月21日
从mysql中读出mid，生成该微博所有评论的url，并放入redis队列中
'''

import sys
sys.path.append('../')

from util import r2mConfig
from util.dbManager2 import dbManager2
from util.header import HEADER
from util.snailLog  import snailLogger
import cookielib
import json
import os
import redis
import time
import urllib2


logger = snailLogger('mysql2Redis_comment.log').get_logger()#/usr/local/src/

class M2RComment(object):

    def __init__(self):
        
        self.__proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
            "host": r2mConfig.ABU_PROXY_HOST,
            "port": r2mConfig.ABU_PROXY_PORT,
            "user": r2mConfig.ABU_PROXY_USER,
            "pass": r2mConfig.ABU_PROXY_PWD,
        }
        # 选择使用代理
        self.__enable_proxy = False
        # 微博评论第一页
        self.__url = 'http://m.weibo.cn/single/rcList?format=cards&id=%s&type=comment&page=%s'
        # mysql连接
        self.__db = dbManager2(dbname=r2mConfig.COMMENT_DB_NAME)
        # redis连接
        self.__redisDb = redis.Redis(
            host=r2mConfig.REDIS_SERVER_IP, port=r2mConfig.REDIS_PORT, db=0, password=r2mConfig.REDIS_PASSWD)
        # self.__redisDb = redis.Redis(
        #     host='127.0.0.1', port=6379, db=0)
        # redis中url队列的名称
        self.__redisUrlName = 'comment:start_urls'
        # 从哪个表中读取数据
        self.__table = 'wblog_1'
        # 记录404的次数，当达到5次时，就重新获取cookie
        self.cnt = 0
        # 重新获取cookie仍然404跳过标记
        self.error404_flag = 0

    # 从mysql获取mid列表
    def get_mids_from_mysql(self):
        try:
            midLs = self.__db.executeSelect(
                'SELECT mid FROM %s WHERE comment_flag = 0 order by id asc limit 0,1' % (self.__table))
            print midLs
        except Exception as e:
            logger.error(
                'Exception in function::: get_midLs_from_mysql -------------------->%s' % str(e))
        else:
            return [str(mid[0]) for mid in midLs]

    # 根据从mysql过去的mid列表生成每条微博所有评论的url，这需要访问第一页来确认最大页数
    def get_urls_based_on_midLs(self):
        urls = []
        cookie = cookielib.MozillaCookieJar()
        cookie.load(os.path.abspath(os.pardir) + '\cookie.txt', ignore_discard=True, ignore_expires=True)
        # 遍历每条微博，访问第一页
        for mid in self.get_mids_from_mysql():
            maxPage = -1  # 最大页数初始化
            maxPageUrl = self.__url % (mid, 1)  # 第一页
            # 构建request 方便加入内容
            request = urllib2.Request(maxPageUrl, headers=HEADER)
            try:
                proxy_handler = urllib2.ProxyHandler(
                    {"http": self.__proxyMeta, 'https': self.__proxyMeta})
                null_proxy_handler = urllib2.ProxyHandler({})
                if self.__enable_proxy:
                    openner = urllib2.build_opener(proxy_handler, urllib2.HTTPCookieProcessor(cookie))
                else:
                    openner = urllib2.build_opener(null_proxy_handler, urllib2.HTTPCookieProcessor(cookie))
                response = openner.open(request)  # ,timeout=5
            except urllib2.HTTPError as e:
                logger.error(
                    'Exception in function::: get_urls_based_on_midLs(error code) mid=%s-------------------->%s' % (mid, str(e.code)))
                if str(e.code) == '404' and self.error404_flag != 1:
                    self.cnt += 1
                if self.cnt == 5:
                    self.cnt = 0
                    self.error404_flag = 1
                    execfile(os.path.abspath(os.pardir) + '/util/login.py')
                    cookie = cookielib.MozillaCookieJar()
                    cookie.load(os.path.abspath(os.pardir) + '\cookie.txt', ignore_discard=True, ignore_expires=True)
                maxPage = -2
                if self.error404_flag == 1:
                    maxPage = -3
                
            except urllib2.URLError as e:
                logger.error(
                    'Exception in function::: get_urls_based_on_midLs(url error) mid=%s-------------------->%s' % (mid, str(e)))
                maxPage = -2
            else:
                logger.info(maxPageUrl)
                try:
                    decodejson = json.loads(response.read())  # 之前不能有print函数
                except Exception as e:
                    logger.error(
                        'Exception in function::: get_urls_based_on_midLs(json data error) mid=%s------------------->%s' % (mid, str(e)))
                    time.sleep(5)
                # 获得mid的url最大页码
                else:
                    # mod/empty表示没有内容
                    if decodejson[1]['mod_type'] == 'mod/empty':
                        maxPage = -1
                    elif 'maxPage' in decodejson[1]:
                        maxPage = decodejson[1]['maxPage']
                    else:
                        maxPage = -1

            # 如果maxPage是-2的话，说明爬网页报错了，就什么都不做
            if maxPage == -2:
                pass
            # 如果maxPage还是-1的话，那么就说明此微博没有评论
            elif maxPage == -1:
                self.__db.execute(
                    'UPDATE %s SET comment_flag = 2 WHERE mid = %s' % (self.__table, mid))
            elif maxPage == -3:
                self.__db.execute(
                    'UPDATE %s SET comment_flag = 3 WHERE mid = %s' % (self.__table, mid))
            else:
                self.__db.execute('UPDATE %s SET comment_flag = 1 WHERE mid = %s'%(self.__table,mid))
            # 将page=1~page=maxPage的url插入urls
            for i in range(maxPage):
                urls.append(self.__url % (mid, str(i + 1)))
        return urls

    # 将urls插入redis队列
    def push_url_to_redis(self):
        try:
            for url in self.get_urls_based_on_midLs():
                self.__redisDb.rpush(self.__redisUrlName, url)
        except Exception as e:
            logger.error(
                'Exception in function::: push_url_to_redis -------------------->%s' % str(e))

    # 获得当前redis数据库中相应的队列中url数量
    def get_redis_url_count(self):
        return self.__redisDb.llen(self.__redisUrlName)

if __name__ == '__main__':
    m2r = M2RComment()
    while True:
        if m2r.get_redis_url_count() < 500:
            m2r.push_url_to_redis()
            time.sleep(3)
#     m2r.push_url_to_redis()