# -*- coding:utf-8 -*-  
'''
Created on 2016年10月1日

@author: MQ
'''
from util.snailLog  import snailLogger
from util.dbManager2 import dbManager2
from util.header import HEADER
import urllib2
import json
import time
import redis


logger = snailLogger('F:\\pytest\\mysql2Redis.log').get_logger()

class mysql2Redis():
    
    def __init__(self):
        self.__proxyHost = 'xxxxx'
        self.__proxyPort = 'xxxx'
        # 代理隧道验证信息
        self.__proxyUser = "xxxxx"
        self.__proxyPass = "xxxxx"
        
        self.__proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
              "host" : self.__proxyHost,
              "port" : self.__proxyPort,
              "user" : self.__proxyUser,
              "pass" : self.__proxyPass,
            }
        #选择使用代理
        self.__enable_proxy = True
        self.__url = 'http://m.weibo.cn/page/json?containerid=100505%s_-_FOLLOWERS&page=%s'
        self.__db = dbManager2(dbname='sina')
        self.__redisDb = redis.Redis(host='223.3.94.211',port= 6379,db=0)
        
    def get_uidLs_from_mysql(self):
        try:
            uidLs = self.__db.executeSelect('select uid from scra_flags_0 where frelation_flag = 0 order by id asc limit 0,10')
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
            logger.error('Exception in function::: get_max_page(error code) -------------------->%s'%str(e.code))
        except urllib2.URLError as e:
            logger.error('Exception in function::: get_max_page(url error) -------------------->%s'+str(e))
        else:
            logger.info(maxPageUrl)
            try:
                decodejson = json.loads(response.read())#之前不能有print函数
            except Exception as e:
                logger.error('Exception in function::: get_max_page(json data error) -------------------->%s'+str(e))
                time.sleep(5)
            else:
            #获得uid的url最大页码
                if decodejson.has_key('ok'):
                    if decodejson.has_key('count'):#如果没有最大页，取count除以10作为最大页面
                        if decodejson['count'] == None:
    #                         maxPage = 0 只记录日志，不返回，默认返回默认maxPage=-1
                            logger.warn('user %s----------------------------> has 0 followers!!'%uid)
                        else:
                            if decodejson.has_key('maxPage'):
                                maxPage = decodejson['cards'][0]['maxPage']
                            else:
                                maxPage = decodejson['count']/10
                
        return maxPage
        
    def fill_url_to_redis(self):
        try:
            #从mysql获得uidLs
            uidLs = self.get_uidLs_from_mysql()
            for uid in uidLs:
                maxPage = self.get_max_page(uid)
                for i in range(maxPage):
                    print self.__url%(uid,i)
#                     self.__redisDb.lpush('myspider:start_urls', self.__url%(uid,i))
        except Exception as e:
            logger.error('Exception in function::: fill_url_to_redis -------------------->%s'%str(e))
#         else:
#             #更新mysql数据库的flag
#             update_sql = 'update scra_flags_0 set frelation_flag = 1 where uid = %s'
#             self.db.execute(update_sql,uidLs)

    #获得当前redis数据库中相应的队列中url数量
    def get_redis_url_count(self,name):
        return self.__redisDb.llen(name)
        
if __name__ == '__main__':
    print '------running------'
    a = mysql2Redis()
    a.fill_url_to_redis()
    print '--------end-----------'
    