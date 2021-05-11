# -*- coding: utf-8 -*-
import json
import time
from scrapy import signals
from scrapy.http import Request
from scrapy_redis.spiders import RedisCrawlSpider
from scrapy_redis.utils import bytes_to_str

from treyler.base import defaults
from treyler.base.utils.mq import KafkaProducer
from treyler.base.utils.monitor import generate_message


class BaseSpider(RedisCrawlSpider):
    """
    抽象类
    """
    platform = None
    producer = None
    producer_topic = None
    use_sorted_set = None
    default_score = None

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        # TODO do we need to overwrite crawler's setting of REDIS_URL? See RedisCrawlSpider.setup_redis
        obj = super(BaseSpider, cls).from_crawler(crawler, *args, **kwargs)
        obj._initialize(crawler)
        return obj

    def _initialize(self, crawler):
        settings = crawler.settings
        # kafka producer
        if not self.producer:
            brokers = settings.get('KAFKA_BROKERS')
            producer_params = settings.get('PRODUCER_PARAMS')
            if not brokers or not producer_params:
                raise AttributeError('')
            st = time.time()
            self.producer = KafkaProducer(brokers=brokers, **producer_params)
            self.logger.info('Producer build connection cost time: %s' % (time.time() - st))

        if not self.producer_topic:
            self.producer_topic = settings.get('KAFKA_TOPICS')

        # use redis's sorted set
        if self.use_sorted_set is None:
            self.use_sorted_set = settings.get('USE_SORTED_SET', defaults.USE_SORTED_SET)
            if self.use_sorted_set:
                self.default_score = settings.get('DEFAULT_SCORE', defaults.DEFAULT_SCORE)

        # platform
        if not self.platform:
            self.platform = settings.get('PLATFORM', defaults.PLATFORM)

        # spider info
        self.logger.info(
            'Spider information:\n\tEnvironment: %s\n\tPlatform: %s\n\tProducer topic: %s',
            settings.get('ENV', 'Undefined'), self.platform, self.producer_topic
        )

        # deprecated method
        # cls = self.__class__
        # if method_is_overridden(cls, BaseSpider, 'xxx'):
        #     warnings.warn(
        #         "BaseSpider.xxx method is deprecated; it "
        #         "won't be called in future releases. Please "
        #         "add tasks outside"
        #     )

        crawler.signals.connect(self.spider_open, signal=signals.spider_opened)

    def requeue(self, task):
        pass
        # xxx ensure duplicate is False
        # score = task.get('extra', {}).get('priority', self.default_score)
        # score += 1
        # self.server.zadd(self.redis_key, {json.dumps(task): score})
        # self.logger.info('Requeue task: %s' % task)

    def next_requests(self):
        """Returns a request to be scheduled or none."""
        use_set = self.settings.getbool('REDIS_START_URLS_AS_SET', False)
        fetch_one = self.server.spop if use_set else self.server.lpop
        # XXX: Do we need to use a timeout here?
        found = 0
        # TODO: Use redis pipeline execution.
        while found < self.redis_batch_size:
            if self.use_sorted_set:
                data = self.server.zrange(self.redis_key, 0, 0)
                if data:
                    data = data[0]
                self.server.zrem(self.redis_key, data)
            else:
                data = fetch_one(self.redis_key)
            if not data:
                # Queue empty.
                break
            req = self.make_request_from_data(data)
            if req:
                yield req
                found += 1
            else:
                self.logger.debug("Request not made from data: %r", data)

        if found:
            self.logger.debug("Read %s requests from '%s'", found, self.redis_key)

    def make_url(self, data):
        """
        override to use
        """
        raise NotImplementedError

    def make_url_from_data(self, data):
        """
        根据redis task类型生成url
        """
        data_type = data.get('type')
        if data_type == 1:
            return data.get('url')
        elif data_type == 0:
            return self.make_url(data.get('data', {}))
        else:
            raise ValueError('Spider: %s, invalid task type: `%s`' % (self.name, data_type))

    def make_request_from_data(self, data):
        """
        override, 区分纯 url 和 dict param
        :param data: bytes
            Message from redis.
        :return:
        """
        if isinstance(data, list):
            data = data[0]
        data = bytes_to_str(data, self.redis_encoding)
        # TODO ensure data type is json loadable
        data = json.loads(data)
        url = self.make_url_from_data(data)
        # 新增request重写
        request = self.make_request(url, data)

        # request callback
        if not getattr(request, 'callback'):
            request = request.replace(callback=self.request_callback)
        if not getattr(request, 'errback'):
            request = request.replace(errback=self.request_error_back)

        # custom depth limit
        custom_max_depth = data.get('depth_limit')
        if custom_max_depth:
            if isinstance(custom_max_depth, int):
                request.meta['custom_max_depth'] = custom_max_depth
            else:
                self.logger.warning('Invalid custom_depth: %s, type: %s', custom_max_depth, type(custom_max_depth))

        # task: {"dont_filter": True}
        # should filter request tag: (True：don't filter；False：filter；default：False)
        dont_filter = data.get('dont_filter', False)
        if dont_filter:
            request = request.replace(dont_filter=True)
        return request

    def make_request(self, url, data):
        """
        make request instance
        """
        return Request(url, meta={'task': data}, callback=self.request_callback, errback=self.request_error_back)

    def request_callback(self, response):
        """
        request callback
        """
        task = response.request.meta.get('task')
        self.send_log(process='download', status=0, task=task)
        return self.parse(response)

    def request_error_back(self, failure):
        """
        request failed callback
        """
        task = failure.request.meta.get('task')
        self.logger.exception('Task: %s, Process [%s] error: %s' % (task, 'download', str(failure)))
        self.send_log(process='download', status=-1, task=task, error=str(failure.value))

    def spider_open(self):
        self.send_log(process='spider_open')

    def send_log(self, process=None, status=0, task=None, **extra):
        msg = generate_message(spider=self,
                               process=process,
                               status=status,
                               task=task,
                               **extra)
        # self.logger.info('from base spider: %s' % msg)
        self.producer.send(self.producer_topic.get('log'), msg)
