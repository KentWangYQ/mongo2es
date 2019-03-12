# -*- coding: utf-8 -*-

from json import dumps
from kafka import KafkaProducer

from config import settings


class Producer(KafkaProducer):

    def __init__(self):
        self._topic_prefix = settings.KAFKA.get('topic_prefix')
        if self._topic_prefix is not None:
            self._topic_prefix += '_'
        KafkaProducer.__init__(self,
                               bootstrap_servers=settings.KAFKA.get('hosts'),
                               client_id=settings.KAFKA.get('client_id'),
                               key_serializer=lambda m: dumps(m).encode('utf-8'),
                               value_serializer=lambda m: dumps(m).encode('utf-8'),
                               acks='all',
                               compression_type='gzip',
                               max_request_size=4194304)

    def send(self, topic, value=None, key=None, headers=None, partition=None, timestamp_ms=None):
        topic = self._topic_prefix + topic
        result = KafkaProducer.send(self, topic=topic, value=value, key=key, headers=headers, partition=partition,
                                    timestamp_ms=timestamp_ms)
        return result


producer = Producer()
