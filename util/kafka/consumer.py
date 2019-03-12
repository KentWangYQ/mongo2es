# -*- coding: utf-8 -*-

from time import sleep

from json import loads
from kafka import KafkaConsumer

from config import settings
from common.event_emitter import EventEmitter


class Consumer(KafkaConsumer):
    @property
    def topic_prefix(self):
        return self._topic_prefix

    @property
    def event_emitter(self):
        return self.__event_emitter

    def __init__(self, topics, group_id=None, enable_auto_commit=True, auto_offset_reset='latest'):
        # 初始化topic_prefix
        self._topic_prefix = settings.KAFKA.get('topic_prefix')
        if self._topic_prefix:
            self._topic_prefix += '_'

        # 初始化event_emitter
        self.__event_emitter = EventEmitter()

        # 初始化consumer
        KafkaConsumer.__init__(self._topic_prefix + topics,
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
            for message in self:
                try:
                    self.__event_emitter.emit('message', message)
                except Exception as e:
                    self.__event_emitter.emit('error', e)

            sleep(1)
