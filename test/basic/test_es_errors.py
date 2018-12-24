# -*- coding: utf-8 -*-

import unittest

from module.elasticsearch.sync.es_errors import exception
from common.elasticsearch.queue_2_elasticsearch.kafka_2_elasticsearch import Kafka2Elasticsearch
from module.elasticsearch.rts import util


class EsErrorsTest(unittest.TestCase):
    def test_index_one(self):
        exception.index_one(Exception('test error'))

    def test_queue_es(self):
        Kafka2Elasticsearch().bulk();

    def test_rts_util(self):
        try:
            raise Exception('test error')
        except Exception as e:
            util.elasticsearch_error(e)
