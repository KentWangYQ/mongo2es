# -*- coding: utf-8 -*-

import pydash as _

from config import settings

from . import redis_2_elasticsearch, kafka_2_elasticsearch


class Queue2ElasticsearchClient(object):
    @property
    def client(self):
        return self.__client

    def __init__(self, mode=None):
        self.__switch = {
            'redis': redis_2_elasticsearch.q2e,
            'kafka': kafka_2_elasticsearch.q2e
        }
        self.__mode = mode or _.get(settings.SYNC, 'rts.mode', 'redis')
        self.__client = _.get(self.__switch, self.__mode)


client = Queue2ElasticsearchClient().client
