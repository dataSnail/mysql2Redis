# -*- coding:utf-8 -*-
'''
Created on 2016年10月3日

@author: MQ
'''
import MySQLdb
from util.snailLog  import snailLogger
logger = snailLogger('dbManager2.log').get_logger()

class dbManager2():
    def __init__(self,dbname,host='223.3.75.180',user='root',passwd='root@123',port=3306,charset='utf8'):
        self.__host = host
        self.__user = user
        self.__passwd = passwd
        self.__port = port
        self.__charset = charset
#         try:
        self.__conn=MySQLdb.connect(host=self.__host,user=self.__user,passwd=self.__passwd,port=self.__port,charset=self.__charset)
        self.__conn.select_db(dbname)
        self.__cur = self.__conn.cursor()
#         except Exception as e:
#             logger.error('dbManager2 Exception in function ::: __init__ e :%s \n'%str(e.reason))

    def executeSelect(self,sql):
        self.__cur.execute(sql)
        resultLs = self.__cur.fetchall()
#         except Exception as e:
#             logger.error('dbManager2 Exception in function ::: execute e :%s \n'%str(e.reason))
#         else:
        return resultLs

    def execute(self,sql):
        self.__cur.execute(sql)
        self.__conn.commit()

    def executemany(self,sql,values):
        self.__cur.executemany(sql,values)
        self.__conn.commit()


    def release(self):
        if self.__cur != None:
            self.__cur.close()
        if self.__conn != None:
            self.__conn.close()
