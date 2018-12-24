# -*- coding: utf-8 -*-
import time
import unittest
from multiprocessing import Pool

from config import FLASK_ENV
from common.log import logger
from common.sentry import SentryClient
from common.elasticsearch.queue_2_elasticsearch.kafka_2_elasticsearch import Kafka2Elasticsearch

from module.elasticsearch.rts import sync_from_queue,real_time_sync

class TestRTS(unittest.TestCase):
    def test_sync_from_queue(self):
        sync_from_queue.sfq.start()

    def test_real_time_sync(self):
        real_time_sync.rts.start()

    def test_clear(self):
        Kafka2Elasticsearch().clear()