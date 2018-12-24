# -*- coding: utf-8 -*-

import pydash as _

from config import settings

from . import elasticsearch_direct_client, elasticsearch_2_redis_client, elasticsearch_2_kafka_client


class ElasticsearchClient(object):
    __mode = _.get(settings.SYNC, 'rts.mode', 'direct')
    __switch = {
        'direct': elasticsearch_direct_client.ElasticsearchDirectClient,
        'redis': elasticsearch_2_redis_client.ElasticsearchRedisClient,
        'kafka': elasticsearch_2_kafka_client.ElasticsearchKafkaClient
    }
    __handler = __switch[__mode] or __switch['direct']

    @property
    def client(self):
        return self.__client

    def __init__(self, mode=None):
        self.__mode = mode or self.__mode
        self.__handler = self.__switch[self.__mode] or self.__switch['direct']
        self.__client = self.__handler(hosts=settings.ELASTICSEARCH.get('hosts'),
                                       timeout=120,
                                       retry_on_timeout=True,
                                       sniff_on_start=True,
                                       sniff_on_connection_fail=True,
                                       sniffer_timeout=1200,
                                       max_retries=3,
                                       )

        self.__client.cluster.health(request_timeout=10)


# multi processor or multi thread can NOT use
es_client = ElasticsearchClient().client
