# -*- coding: utf-8 -*-

import pydash as _
import arrow
from time import sleep

from common.log import logger
from common.redis_client import redis_client
from common.mongo.bson_c import json_util

from .base import Base


class Redis2Elasticsearch(Base):
    def bulk(self):
        while True:
            try:
                start_time = arrow.utcnow()
                coll = redis_client.lrange(self.queue_key, 0, self.bulk_size - 1)
                if coll and len(coll) > 0:
                    for json in coll:
                        r = json_util.loads(json)
                        self.data_handlers[_.get(r, 'op')](r)

                    self.generate_bulk_body()
                    self.do_task()

                    for i in range(0, len(coll)):
                        redis_client.lpop(self.queue_key)

                logger.info('==== bulk indexed:{0} ===='.format(len(coll)))

                used_time = (arrow.utcnow() - start_time).seconds
                if used_time < self.wait:
                    sleep(self.wait - used_time)
            except Exception as e:
                self.event_emitter.emit('error', e)

    def clear(self):
        redis_client.delete(self.queue_key)


q2e = Redis2Elasticsearch()
