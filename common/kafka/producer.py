# -*- coding: utf-8 -*-

from json import dumps
from kafka import KafkaProducer

from config import settings


class Producer(object):
    _topic_prefix = settings.KAFKA.get('topic_prefix')
    if _topic_prefix is not None:
        _topic_prefix += '_'

    def __init__(self):
        self._producer = KafkaProducer(bootstrap_servers=settings.KAFKA.get('hosts'),
                                       client_id=settings.KAFKA.get('client_id'),
                                       key_serializer=lambda m: dumps(m).encode('utf-8'),
                                       value_serializer=lambda m: dumps(m).encode('utf-8'),
                                       acks='all',
                                       compression_type='gzip',
                                       max_request_size=4194304)

    def send(self, topic, value, key=None, partition=None, timestamp_ms=None):
        topic = self._topic_prefix + topic
        result = self._producer.send(topic, key=key, value=value, partition=partition, timestamp_ms=timestamp_ms)
        return result

    def close(self, timeout=None):
        self._producer.close(timeout=timeout)

    def flush(self, timeout=None):
        self._producer.flush(timeout=timeout)


producer = Producer()
