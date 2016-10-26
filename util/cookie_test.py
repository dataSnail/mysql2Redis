# -*- coding:utf-8 -*-
'''
Created on 2016年10月26日
test cookie
'''
import os
import urllib2
import cookielib


class Ctest(object):
    def __init__(self):
        self.__proxyHost = 'proxy.abuyun.com'
        self.__proxyPort = '9010'
        # 代理隧道验证信息
        self.__proxyUser = "H5S031HK5GAI638P"
        self.__proxyPass = "0451B74483012582"

        self.__proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
            "host": self.__proxyHost,
            "port": self.__proxyPort,
            "user": self.__proxyUser,
            "pass": self.__proxyPass,
        }
        # 选择使用代理
        self.__enable_proxy = False
        # 微博评论第一页
        self.__url = 'http://m.weibo.cn/single/rcList?format=cards&id=3914102592235131&type=comment&page=1'

    # 根据从mysql过去的mid列表生成每条微博所有评论的url，这需要访问第一页来确认最大页数
    def test(self):
        cookie = cookielib.MozillaCookieJar()
        cookie.load(os.path.abspath(os.pardir) + '\cookie.txt', ignore_discard=True, ignore_expires=True)
        agent = 'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.110 Safari/537.36'
        headers = {
            "Host": "m.weibo.cn",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            'User-Agent': agent
        }
        request = urllib2.Request(self.__url, headers=headers)
        try:
            proxy_handler = urllib2.ProxyHandler(
                {"http": self.__proxyMeta, 'https': self.__proxyMeta})
            null_proxy_handler = urllib2.ProxyHandler({})
            if self.__enable_proxy:
                openner = urllib2.build_opener(proxy_handler, urllib2.HTTPCookieProcessor(cookie))
            else:
                openner = urllib2.build_opener(null_proxy_handler, urllib2.HTTPCookieProcessor(cookie))

            response = openner.open(request)  # ,timeout=5
            print response.read()
        except urllib2.HTTPError as e:
            print(
                'Exception in function::: get_urls_based_on_midLs(error code) %s' % (str(e.code)))
        except urllib2.URLError as e:
            print(
                'Exception in function::: get_urls_based_on_midLs(url error) %s' % (str(e)))
        print 'yes'

        # 另一种使用cookie访问网页的方法
        # load_cookiejar = cookielib.MozillaCookieJar()
        # load_cookiejar.load('cookie.txt', ignore_discard=True, ignore_expires=True)
        # load_cookies = requests.utils.dict_from_cookiejar(load_cookiejar)
        # session = requests.session()
        # session.cookies = requests.utils.cookiejar_from_dict(load_cookies)
        # ht = session.get(self.__url, headers=headers)
        # print ht


if __name__ == '__main__':
    ct = Ctest()
    ct.test()
