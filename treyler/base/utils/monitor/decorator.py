# -*- coding: utf-8 -*-
import logging
from functools import wraps
from types import GeneratorType
from scrapy.http import Request, Response

from treyler.base.spiders import BaseSpider
from treyler.base.defaults import KAFKA_TOPICS
from treyler.base.utils.monitor.message import generate_message

logger = logging.getLogger(__name__)

default_topic = KAFKA_TOPICS.get('log', 'crawl_log')


def monitor_dec(process=None, **monitor_kwargs):
    def gen_wrapper(generator, spider, message):
        try:
            for i in generator:
                yield i
                message['status'] = 0
                monitor_send(spider, message)
        except Exception as e:
            logger.exception('Task: %s, Process [%s] error: %s' % (message.get('task'), process, str(e)))
            message['status'] = -1
            message['extra'].update({'error': str(e)})
            monitor_send(spider, message)

    def wrapper(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            is_generator = False
            spider = None
            task = None
            msg = generate_message(process=process, **monitor_kwargs)
            arg_list = tuple(args) + tuple(kwargs.values())
            for arg in arg_list:
                if isinstance(arg, BaseSpider):
                    spider = arg
                    msg['platform'] = getattr(arg, 'platform', None)
                    msg['spider'] = getattr(arg, 'name', None)
                if isinstance(arg, (Request, Response)) and not task:
                    task = arg.meta.get('task', None)
                # TODO get task info from item when process is pipeline
            msg['task'] = task
            try:
                result = func(*args, **kwargs)
                if isinstance(result, GeneratorType):
                    is_generator = True
                    result = gen_wrapper(result, spider, msg)
                return result
            except Exception as e:
                logger.exception('Task: %s, Process [%s] error: %s' % (task, process, str(e)))
                msg['status'] = -1
                msg['extra'].update({'error': str(e)})
            finally:
                if not is_generator:
                    monitor_send(spider, msg)
        return wrapped
    return wrapper


def monitor_send(spider, message):
    assert isinstance(spider, BaseSpider)
    # logger.info('from dec: %s' % message)
    topic = spider.producer_topic.get('log', default_topic)
    spider.producer.send(topic, message)
