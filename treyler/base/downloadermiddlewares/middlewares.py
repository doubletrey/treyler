# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

import random
from ..exceptions import RetryException, GiveUpRequest
from scrapy.downloadermiddlewares.retry import RetryMiddleware


class CFRetryMiddleware(RetryMiddleware):
    EXCEPTIONS_TO_RETRY = RetryMiddleware.EXCEPTIONS_TO_RETRY + (RetryException, TimeoutError)

    def give_up(self, request, response, spider, reason):
        raise GiveUpRequest(reason)


class ProxyMiddleware(object):
    def __init__(self, settings):
        self.settings = settings
        self.proxies = None
        self._initialize()

    def _initialize(self):
        self.proxies = self.gener_proxies()

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def gener_proxies(self):
        """
        子类重写该方法，初始化代理池
        :return:
        """
        raise NotImplementedError

    def get_proxy(self):
        """
        子类重写该方法，为request选择代理
        :return:
        """
        return random.choice(self.proxies)

    def process_request(self, request, spider):
        # 重选代理
        request.headers.pop('Proxy-Authorization', None)
        request.meta['proxy'] = self.get_proxy()
