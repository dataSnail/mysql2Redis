# -*- coding:utf-8 -*-
'''
Created on 2016年10月21日
从mysql中读出uid，生成该用户所有的微博页面url，并放入redis队列中
'''
import sys
sys.path.append('../')

from util.snailLog  import snailLogger
from util.dbManager2 import dbManager2
from util.header import HEADER
import urllib2
import json
import time
import redis
from util import r2mConfig

logger = snailLogger('mysql2Redis_wblog.log').get_logger()#/usr/local/src/

class M2RWblog(object):

    __mark403 = False

    def __init__(self):
        self.__proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
            "host": r2mConfig.ABU_PROXY_HOST,
            "port": r2mConfig.ABU_PROXY_PORT,
            "user": r2mConfig.ABU_PROXY_USER,
            "pass": r2mConfig.ABU_PROXY_PWD,
        }
        # 选择使用代理
        self.__enable_proxy = r2mConfig.WBLOG_PROXY
        # 用户微博第一页
        self.__url = r2mConfig.WBLOG_URL
        # mysql连接
        self.__db = dbManager2(dbname=r2mConfig.WBLOG_DB_NAME)
        # redis连接
        self.__redisDb = redis.Redis(
            host=r2mConfig.REDIS_SERVER_IP, port=r2mConfig.REDIS_PORT, db=0, password=r2mConfig.REDIS_PASSWD)
        # redis中url队列的名称
        self.__redisUrlName = r2mConfig.WBLOG_RUN
        # 从哪个表中读取数据
        self.__table = r2mConfig.WBLOG_TABLE_FROM

    # 从mysql获取uid列表
    def get_uidLs_from_mysql(self):
        try:
            uidLs = self.__db.executeSelect(
                'SELECT uid FROM %s WHERE wblog_flag = 0 order by id asc limit 0,1' % (self.__table))
        except Exception as e:
            logger.error(
                'Exception in function::: get_uidLs_from_mysql -------------------->%s' % str(e))
        else:
            return [str(uid[0]) for uid in uidLs]

    # 根据从mysql过去的uid列表生成每个用户的所有微博url，这需要访问第一页来确认最大页数
    def get_urls_based_on_uidLs(self):
        urls = []
        # 遍历每个用户，访问每个人的第一页
        for uid in self.get_uidLs_from_mysql():
            if self.__mark403:#上次请求有403错误，此次请求代理换ip
                HEADER['Proxy-Switch-Ip'] = "yes"
                self.__mark403 = False
            maxPage = -1  # 最大页数初始化
            maxPageUrl = self.__url % (uid, 1)  # 第一页
            # 构建request 方便加入内容
            request = urllib2.Request(maxPageUrl, headers=HEADER)
            try:
                proxy_handler = urllib2.ProxyHandler(
                    {"http": self.__proxyMeta, 'https': self.__proxyMeta})
                null_proxy_handler = urllib2.ProxyHandler({})
                if self.__enable_proxy:
                    openner = urllib2.build_opener(proxy_handler)
                else:
                    openner = urllib2.build_opener(null_proxy_handler)
                response = openner.open(request)  # ,timeout=5

            except urllib2.HTTPError as e:
                logger.error(
                    'Exception in function::: get_urls_based_on_uidLs(error code) uid=%s-------------------->%s' % (uid, str(e.code)))
                maxPage = -2
                if e.code == 429:#请求过于频繁
                    time.sleep(2)
                if e.code == 403:#ip被远程服务器拒绝，应该请求下次换ip，置__mark403 = True
                    self.__mark403 = True

            except urllib2.URLError as e:
                logger.error(
                    'Exception in function::: get_urls_based_on_uidLs(url error) uid=%s-------------------->%s' % (uid, str(e)))
                maxPage = -2
            else:
                logger.info(maxPageUrl)
                try:
                    decodejson = json.loads(response.read())  # 之前不能有print函数
                except Exception as e:
                    logger.error(
                        'Exception in function::: get_urls_based_on_uidLs(json data error) uid=%s------------------->%s' % (uid, str(e)))
                    time.sleep(5)
                # 获得uid的url最大页码
                else:
                    if decodejson.has_key('ok'):
                        # mod/empty表示没有内容
                        if decodejson['cards'][0]['mod_type'] == 'mod/empty':
                            maxPage = -1
                        elif 'maxPage' in decodejson['cards'][0]:
                            maxPage = decodejson['cards'][0]['maxPage']
                        elif 'count' in decodejson and decodejson['count'] != None:
                            # +9保证精确计算出maxPage
                            maxPage = int(decodejson['count'] + 9) / 10
                        else:
                            maxPage = -1

            # 如果maxPage是-2的话，说明爬网页报错了，就什么都不做
            if maxPage == -2:
                pass
            # 如果maxPage还是-1的话，那么就说明此用户没有微博
            elif maxPage == -1:
                self.__db.execute(
                    'UPDATE %s SET wblog_flag = 2 WHERE uid = %s' % (self.__table, uid))
            else:
                self.__db.execute(
                    'UPDATE %s SET wblog_flag = 1 WHERE uid = %s' % (self.__table, uid))
            # 将page=1~page=maxPage的url插入urls
            for i in range(maxPage):
                urls.append(self.__url % (uid, str(i + 1)))
        return urls

    # 将urls插入redis队列
    def push_url_to_redis(self):
        try:
            for url in self.get_urls_based_on_uidLs():
                self.__redisDb.rpush(self.__redisUrlName, url)
        except Exception as e:
            logger.error(
                'Exception in function::: push_url_to_redis -------------------->%s' % str(e))

    # 获得当前redis数据库中相应的队列中url数量
    def get_redis_url_count(self):
        return self.__redisDb.llen(self.__redisUrlName)

if __name__ == '__main__':
    m2r = M2RWblog()
    while True:
        if m2r.get_redis_url_count() < 1000:
            m2r.push_url_to_redis()
        else:
            time.sleep(9)
