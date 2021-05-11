# -*- coding: utf-8 -*-

import json
import logging
from kafka import KafkaProducer

logger = logging.getLogger(__name__)


class Producer(object):
    def __init__(self, brokers, **kwargs):
        if 'retries' not in kwargs:
            # 默认重试次数：5
            kwargs['retries'] = 5
        self.brokers = brokers
        self.conn_params = kwargs
        self.client = KafkaProducer(bootstrap_servers=brokers, **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def send(self, topic, msg, **kwargs):
        if isinstance(msg, (list, tuple, dict)):
            value = json.dumps(msg).encode('utf-8')
        else:
            value = msg
        try:
            self.client.send(topic=topic, value=value, **kwargs)
            self.client.flush()
        except Exception as e:
            logger.exception('Kafka send %(msg)s error: %(error)', {'msg': value, 'error': e})

    def close(self):
        self.client.close()
