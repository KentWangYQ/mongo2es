# -*- coding: utf-8 -*-

from elasticsearch.exceptions import ElasticsearchException
from config import FLASK_ENV

from common.log import logger
from common import event_emitter
from common.sentry import SentryClient
from common.elasticsearch.queue_2_elasticsearch.queue_2_elasticsearch_client import client

from . import util


class SyncFromQueue(object):
    __done = None
    __sentry_client = SentryClient(ignore_exceptions=['KeyboardInterrupt'])

    def start(self):
        @event_emitter.on(client.event_emitter, 'error')
        def on_error(error):
            if isinstance(error, ElasticsearchException):
                util.elasticsearch_error(error)
            else:
                if FLASK_ENV != 'development':
                    self.__sentry_client.capture_exception()

            logger.exception(error)

        client.bulk()


sfq = SyncFromQueue()
