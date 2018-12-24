# -*- coding: utf-8 -*-

import pydash as _
import fire
import arrow
import json

from config import settings

from common.etcd_config import mongo2es
from common.log import logger

mode = _.get(settings.SYNC, 'rts.mode')
settings.SYNC['rts']['mode'] = 'direct'

from common.elasticsearch.queue_2_elasticsearch.queue_2_elasticsearch_client import Queue2ElasticsearchClient

from module.elasticsearch import const
from module import elasticsearch


class Sync(object):
    def index_all(self):
        # 清除queue遗留消息
        client = Queue2ElasticsearchClient(mode=mode).client
        if client:
            client.clear()

        # 记录当前时间，后续作为实时同步启动的时间点
        now = arrow.utcnow().timestamp

        # 全量同步
        self.__index_all()

        # 设置停止时间为实时同步开始时间
        mongo2es.set(const.rts_reset_key, json.dumps({"ts": {"time": now, "inc": 1}}))

    def __index_all(self):
        elasticsearch.es_errors.index()
        elasticsearch.tracks.index()
        elasticsearch.car_change_plans.index()

    def index(self, *indices):
        if isinstance(indices, tuple):
            for index in indices:
                if not hasattr(elasticsearch, index):
                    logger.warning('elasticsearch has no index "{0}"'.format(index))
                    return
            for index in indices:
                getattr(elasticsearch, index).index()
        else:
            logger.error('Invalidate type, indices must be str or tuple.')


fire.Fire(Sync)
