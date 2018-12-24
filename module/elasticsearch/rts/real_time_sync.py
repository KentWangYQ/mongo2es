# -*- coding: utf-8 -*-

import bson
import json
import arrow

from elasticsearch.exceptions import ElasticsearchException
from config import settings
from config import FLASK_ENV

from common.log import logger
from common.mongo import oplog
from common import event_emitter
from common.sentry import SentryClient

from common.etcd_config import mongo2es

from module.elasticsearch import const
from module.elasticsearch.sync import car_change_plans, tracks

from . import util


class RealTimeSync(object):
    __done = None
    __sentry_client = SentryClient(ignore_exceptions=['KeyboardInterrupt'])

    @property
    def key(self):
        return const.rts_key

    @property
    def reset_key(self):
        return const.rts_reset_key

    __mongo_oplog = None

    def __init__(self):
        pass

    def start(self):
        self.__mongo_oplog = self.__create_oplog()
        self.__register()
        self.__mongo_oplog.tail()

    def stop(self):
        if self.__mongo_oplog:
            self.__mongo_oplog.close()

    def __create_oplog(self):
        try:
            ts = None
            ts_sync_reset = mongo2es.get(self.reset_key)
            if ts_sync_reset:
                mongo2es.delete(self.reset_key)
                ts = json.loads(ts_sync_reset).get('ts')
            if not ts:
                ts = json.loads(mongo2es.get(self.key)).get('ts')

            time = ts.get('time')
            inc = ts.get('inc')
        except Exception as ex:
            logger.info("reading last timestamp error")
            logger.error(ex)
            raise Exception("reading last timestamp error,detail info:" + ex.message)

        self.__done = {"ts": {"time": time, "inc": inc}}

        _mongo_oplog = oplog.MongoOplog(
            uri=settings.MONGO.get('oplog_uri'),
            ts=bson.Timestamp(time, inc),
            ns=settings.MONGO.get('oplog_ns'),
            exclude_ns=settings.MONGO.get('oplog_ns_exclude'),
            include_ns=settings.MONGO.get('oplog_ns_include'),
            include_ops=['i', 'u', 'd'])

        @event_emitter.on(_mongo_oplog.event_emitter, 'data')
        def on_data(data):
            logger.info({'a_ts': data.get('ts'), 'b_op': data.get('op'), 'c_ns': data.get('ns'),
                         'd_time': arrow.get(data.get('ts').time).format('YYYY-MM-DD hh:mm:ss.SSS')})
            mongo2es.set(self.key, json.dumps(self.__done))
            self.__done = {"ts": {"time": data['ts'].time, "inc": data['ts'].inc}}

        @event_emitter.on(_mongo_oplog.event_emitter, 'error')
        def on_error(error, data):
            if isinstance(error, ElasticsearchException):
                util.elasticsearch_error(error)
            else:
                if FLASK_ENV != 'development':
                    self.__sentry_client.capture_exception()

            logger.info(data)
            logger.exception(error)

        return _mongo_oplog

    def __register(self):
        car_change_plans.rt_index(self.__mongo_oplog)
        tracks.rt_index(self.__mongo_oplog)


rts = RealTimeSync()
