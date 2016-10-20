# -*- coding:utf-8 -*-
'''
Created on 2016年10月1日

@author: MQ
'''
import sys
from time import sleep
sys.path.append('../')

from util.snailLog  import snailLogger
from util.dbManager2 import dbManager2
from util.header import HEADER
import urllib2
import json
import time
import redis


logger = snailLogger('mysql2Redis.log').get_logger()#/usr/local/src/

class mysql2Redis():

    def __init__(self):
        self.__proxyHost = 'proxy.abuyun.com'
        self.__proxyPort = '9010'
        # 代理隧道验证信息
        self.__proxyUser = "H5S031HK5GAI638P"
        self.__proxyPass = "0451B74483012582"

        self.__proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
              "host" : self.__proxyHost,
              "port" : self.__proxyPort,
              "user" : self.__proxyUser,
              "pass" : self.__proxyPass,
            }
        #选择使用代理
        self.__enable_proxy = False
        self.__url = 'http://m.weibo.cn/page/json?containerid=100505%s_-_FOLLOWERS&page=%s'
        self.__db = dbManager2(dbname='sina')
        self.__redisDb = redis.Redis(host='223.3.94.211',port= 6379,db=0,password='redis123')

    def get_uidLs_from_mysql(self):
        try:
            uidLs = self.__db.executeSelect('select uid from scra_flags_0 where frelation_flag = 0 order by id asc limit 0,1')
        except Exception as e:
            logger.error('Exception in function::: get_uidLs_from_mysql -------------------->%s'%str(e))
        else:
            return [str(uid[0]) for uid in uidLs]


    def get_max_page(self,uid):
        maxPage = -1
        maxPageUrl = self.__url%(uid,1)
        #构建request 方便加入内容
        request = urllib2.Request(maxPageUrl,headers = HEADER)
        try:
            proxy_handler = urllib2.ProxyHandler({"http" : self.__proxyMeta,'https':self.__proxyMeta})
            null_proxy_handler = urllib2.ProxyHandler({})
            if self.__enable_proxy:
                openner = urllib2.build_opener(proxy_handler)
            else:
                openner = urllib2.build_opener(null_proxy_handler)
            response = openner.open(request)#,timeout=5
        except urllib2.HTTPError as e:
            logger.error('Exception in function::: get_max_page(error code) uid=%s-------------------->%s'%(uid,str(e.code)))
        except urllib2.URLError as e:
            logger.error('Exception in function::: get_max_page(url error) uid=%s-------------------->%s'%(uid,str(e)))
        else:
            logger.info(maxPageUrl)
            try:
                decodejson = json.loads(response.read())#之前不能有print函数
            except Exception as e:
                logger.error('Exception in function::: get_max_page(json data error) uid=%s------------------->%s'%(uid,str(e)))
                time.sleep(5)
            else:
            #获得uid的url最大页码
                if decodejson.has_key('ok'):
                    if decodejson.has_key('count'):#如果没有最大页，取count除以10作为最大页面
                        if decodejson['count'] == None:
    #                         maxPage = 0 只记录日志，不返回，默认返回默认maxPage=-1
                            logger.warn('user %s----------------------------> has 0 followers!!'%uid)
                        else:
                            if decodejson['cards'][0].has_key('maxPage'):
                                maxPage = decodejson['cards'][0]['maxPage']
                            else:
                                maxPage = decodejson['count']/10+1

        return maxPage

    def fill_url_to_redis(self):
        try:
            #从mysql获得uidLs
            uidLs = self.get_uidLs_from_mysql()
            for uid in uidLs:
                maxPage = self.get_max_page(uid)
                for i in range(maxPage):
                    # print self.__url%(uid,i)
                    self.__redisDb.rpush('frelation:start_urls', self.__url%(uid,i))
        except Exception as e:
            logger.error('Exception in function::: fill_url_to_redis -------------------->%s'%str(e))
#         else:
#             #更新mysql数据库的flag
#             update_sql = 'update scra_flags_0 set frelation_flag = 1 where uid = %s'
#             self.__db.executemany(update_sql,uidLs)

    #获得当前redis数据库中相应的队列中url数量
    def get_redis_url_count(self,name):
        return self.__redisDb.llen(name)

    def tesRedis(self):
        for i in range(10):
            print i
            self.__redisDb.rpush('myspider:start_urls','http://%s'%i)

if __name__ == '__main__':
    print '------running------'
#     sleep(60)
    a = mysql2Redis()
    a.fill_url_to_redis()
    print '--------end-----------'

