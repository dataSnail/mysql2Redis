# -*- coding:utf-8 -*-
'''
Created on 2016年10月26日

@author: MQ
'''
#redis 服务器地址
REDIS_SERVER_IP = '223.3.94.145'
#redis 端口
REDIS_PORT = 6379
#redis 密码
REDIS_PASSWD = 'redis123'
#mysql 服务器地址
MYSQL_SERVER_IP = '223.3.75.180'
#mysql 服务器用户名
MYSQL_USERNAME = 'root'
#mysql 服务器密码
MYSQL_PWD = 'root@123'

#----------------代理------------------#
ABU_PROXY_HOST = 'proxy.abuyun.com'

ABU_PROXY_PORT = "9010"

ABU_PROXY_USER = "H5S031HK5GAI638P"

ABU_PROXY_PWD = "0451B74483012582"
#---------------------------------------#

#----------------wblog-------------------#
# 数据库
WBLOG_DB_NAME = 'sina'
# 从哪个表读出用户id
WBLOG_TABLE_FROM = 'scra_flags_0'
# 微博url
WBLOG_URL = 'http://m.weibo.cn/page/json?containerid=100505%s_-_WEIBO_SECOND_PROFILE_WEIBO&itemid=&page=%s'
# redis中url队列的名称
WBLOG_RUN = 'wblog:start_urls'
# 是否使用代理
WBLOG_PROXY = True
#---------------------------------------#

#----------------comment-------------------#
COMMENT_DB_NAME = 'sina_comment'
#---------------------------------------#
