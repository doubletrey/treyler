# -*- coding: utf-8 -*-
import logging
import redis
from twisted.enterprise import adbapi

logger = logging.getLogger(__name__)


class BasePipeline(object):
    @classmethod
    def from_crawler(cls, crawler):
        obj = cls()
        obj.initialize(crawler)
        return obj

    def initialize(self, crawler):
        pass

    def open_spider(self, spider):
        """
        :param spider:
        :return:
        """
        pass

    def close_spider(self, spider):
        """
        close conn
        :param spider:
        :return:
        """
        pass

    def save(self, item, spider):
        """
        # mq send
        :param item:
        :param spider:
        :return:
        """
        raise NotImplementedError

    def extend(self, item, spider):
        """
        # override to use
        """
        raise NotImplementedError

    def save_and_extend(self, item, spider):
        self.save(item, spider)
        self.extend(item, spider)


class TwistedMysqlPipeline(BasePipeline):
    db_conn_params = None
    db_pool = None
    redis_url = None
    redis_cli = None

    def initialize(self, crawler):
        settings = crawler.settings
        if not self.db_conn_params:
            params = settings.get('MYSQL_CONN_PARAMS')
            if not params:
                raise ValueError("`MYSQL_CONN_PARAMS` is required in settings")
            self.db_conn_params = params
        if not self.redis_url:
            self.redis_url = settings.get('REDIS_URL', 'redis://localhost:6379')

    def open_spider(self, spider):
        # db pool
        if self.db_conn_params and not self.db_pool:
            self.db_pool = adbapi.ConnectionPool('pymysql',
                                                 cp_max=2,
                                                 cp_reconnect=True,
                                                 **self.db_conn_params)

        # redis client session
        if not self.redis_cli:
            from scrapy_redis.spiders import RedisCrawlSpider
            if isinstance(spider, RedisCrawlSpider):
                self.redis_cli = spider.server
            else:
                # xxx: create redis client session
                self.redis_cli = redis.StrictRedis.from_url(self.redis_url)

    def close_spider(self, spider):
        if self.db_pool:
            self.db_pool.close()

    def save(self, item, spider):
        """
        Asynchronous operations
        """
        query = self.db_pool.runInteraction(self.operate_db, item)
        query.addErrback(self._handle_error)

    def operate_db(self, cur, item):
        """
        database operations
        override to use
        """
        raise NotImplementedError

    def _handle_error(self, failue):
        # TODO: monitor
        logger.warning('Database operation failed: %s', failue)
