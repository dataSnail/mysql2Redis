# -*- coding:utf-8 -*-
'''
Created on 2016年10月1日

@author: MQ
'''
import sys
sys.path.append('../')
from util import r2mConfig

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
        self.__enable_proxy = True
        self.__url = 'http://m.weibo.cn/page/json?containerid=100505%s_-_FOLLOWERS&page=%s'
        #数据库连接不上，停止运行程序30min = 1800s
        try:
            self.__db = dbManager2(dbname='sina')
            self.__redisDb = redis.Redis(host=r2mConfig.REDIS_SERVER_IP,port= 6379,db=0,password=r2mConfig.REDIS_PASSWD)
        except Exception as e:
            logger.error('Exception in __init__ e:::::%s  and sleep at :::%s-----1800s'%(str(e),time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))))
            time.sleep(1800)

    def get_uidLs_from_mysql(self):
        try:
            sql = 'select uid from scra_flags_0 where frelation_flag = 0 order by id asc limit 0,1'
#             sql_bak = 'select uid from scra_flags_0 where uid = 5579940613'#frelation_flag = 1 and id >11984 and id <19333 '#1020增补最后一页
            uidLs = self.__db.executeSelect(sql)
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
            maxPage = -2
            if e.code == 429:#请求过于频繁
                time.sleep(2)
        except urllib2.URLError as e:
            maxPage = -2
            logger.error('Exception in function::: get_max_page(url error) uid=%s-------------------->%s'%(uid,str(e)))
        else:
            try:
                decodejson = json.loads(response.read())#之前不能有print函数
            except Exception as e:
                logger.error('Exception in function::: get_max_page(json data error) uid=%s------------------->%s'%(uid,str(e)))
                maxPage = -2
#                 time.sleep(5)
            else:
            #获得uid的url最大页码
                if decodejson.has_key('ok'):
                    if decodejson.has_key('count'):#如果没有最大页，取count除以10作为最大页面
                        if decodejson['count'] == None or decodejson['count'] == 0:
    #                         maxPage = 0 只记录日志，不返回，默认返回默认maxPage=-1
                            logger.warn('user %s----------------------------> has 0 followers or can not get content!!'%uid)
                        else:
                            if decodejson['cards'][0].has_key('maxPage'):
                                maxPage = decodejson['cards'][0]['maxPage']
                            else:
                                maxPage = (decodejson['count']-1)/10+1#decodejson['count']大于0
                                logger.warn('user %s----------------------------> has no maxPage!!! count:::%s,count/10 +1 instead :::%s'%(uid,decodejson['count'],maxPage))
                logger.info('function::::get_max_page-------------uid::::%s:::maxPage:::::%s'%(uid,maxPage))#日志文件完善20161020
        return maxPage

    def fill_url_to_redis(self):
        if self.get_redis_url_count('frelation:start_urls')<8000:
            try:
                #从mysql获得uidLs
                uidLs = self.get_uidLs_from_mysql()
                countLT0 =[]#防止真的没有关注者的用户干扰程序，保存有关注者但是取不到的用户和确实没有关注者的用户列表，更新标记2
                if uidLs != None:#数据库运行正常
                    updateUserLs = uidLs
                    
                    for uid in updateUserLs:
                        maxPage = self.get_max_page(uid)
                        if maxPage > 0:
    #                         print self.__url%(uid,maxPage)#1020增补最后一页
    #                         self.__redisDb.rpush('frelation:start_urls', self.__url%(uid,maxPage))#1020增补最后一页
                            for i in range(1,maxPage+1):#range的最大页数从1到maxPage
                                self.__redisDb.rpush('frelation:start_urls', self.__url%(uid,i))
                        elif maxPage == -2:#处理页码出现异常，则从uidLs中删除此uid，此种用户不更新爬取标记
                            uidLs.remove(uid)
                            logger.error('uid::::::%s do nothing because maxPage < 0 (maxPage=-2)'%uid)
                        else:#没有取得最大页码，则从uidLs中删除此uid
                            uidLs.remove(uid)
                            countLT0.append(str(uid))
                            logger.warn('uid::::::%s is added in countLT0 because maxPage < 0 (maxPage=-1)'%uid)
                        
            except Exception as e:
                logger.error('Exception in function::: fill_url_to_redis uidLs:::%s-------------------->%s'%(','.join(uidLs),str(e)))
            else:
                if len(uidLs)>0:
                    #正常用户，更新mysql数据库的flag
                    update_sql = 'update scra_flags_0 set frelation_flag = 1 where uid = %s'
                    self.__db.executemany(update_sql,uidLs)
                if len(countLT0) > 0:
                    #异常用户更新标记为2
                    update_sql = 'update scra_flags_0 set frelation_flag = 2 where uid = %s'
                    self.__db.executemany(update_sql,countLT0)
        

    #获得当前redis数据库中相应的队列中url数量
    def get_redis_url_count(self,name):
        return self.__redisDb.llen(name)

    def tesRedis(self):
        for i in range(10):
            print i
            self.__redisDb.rpush('myspider:start_urls','http://%s'%i)

if __name__ == '__main__':
#     print '------running------'
    if len(sys.argv)==2:
        if int(sys.argv[1])>=0 and int(sys.argv[1])<=10:
            a = mysql2Redis()
            while 1:
                a.fill_url_to_redis()
                time.sleep(int(sys.argv[1]))
    else:
        print 'use  "python '+sys.argv[0]+' [second]" to start!'
#     print '--------end-----------'
#     a = mysql2Redis()
#     print a.get_redis_url_count("frealtion:start_urls") <10000
