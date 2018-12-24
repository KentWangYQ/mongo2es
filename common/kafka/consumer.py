# -*- coding: utf-8 -*-

from time import sleep

from json import loads
from kafka import KafkaConsumer

from config import settings
from common import event_emitter


class Consumer(object):
    @property
    def topic_prefix(self):
        return self._topic_prefix

    @property
    def consumer(self):
        return self._consumer

    @property
    def event_emitter(self):
        return self.__event_emitter

    def __init__(self, topics, group_id=None, enable_auto_commit=True, auto_offset_reset='latest'):
        # 初始化topic_prefix
        self._topic_prefix = settings.KAFKA.get('topic_prefix')
        if self._topic_prefix:
            self._topic_prefix += '_'

        # 初始化event_emitter
        self.__event_emitter = event_emitter.EventEmitter()

        # 初始化consumer
        self._consumer = KafkaConsumer(self._topic_prefix + topics,
                                       bootstrap_servers=settings.KAFKA.get('hosts'),
                                       client_id=settings.KAFKA.get('client_id'),
                                       group_id=group_id,
                                       key_deserializer=lambda m: loads(m.decode('utf-8')),
                                       value_deserializer=lambda m: loads(m.decode('utf-8')),
                                       enable_auto_commit=enable_auto_commit,
                                       auto_offset_reset=auto_offset_reset,
                                       session_timeout_ms=30000
                                       )

    def tail(self):
        while True:
            for message in self._consumer:
                try:
                    self.__event_emitter.emit('message', message)
                except Exception as e:
                    self.__event_emitter.emit('error', e)

            sleep(1)

    def poll(self, timeout_ms=0, max_records=None):
        return self._consumer.poll(timeout_ms=timeout_ms, max_records=max_records)

    def commit(self, offsets=None):
        self._consumer.commit(offsets=offsets)

    def commit_async(self, offsets=None, callback=None):
        self._consumer.commit_async(offsets=offsets, callback=callback)

    def close(self, autocommit=True):
        self._consumer.close(autocommit=autocommit)

    def seek_to_beginning(self, *partitions):
        self._consumer.seek_to_beginning(*partitions)

    def seek_to_end(self, *partitions):
        self._consumer.seek_to_end(*partitions)

    def partitions_for_topic(self, topic):
        return self._consumer.partitions_for_topic(topic=topic)

    def assignment(self):
        return self._consumer.assignment()

    def end_offsets(self, partitions):
        return self._consumer.end_offsets(partitions)

    def position(self, partition):
        return self._consumer.position(partition)
