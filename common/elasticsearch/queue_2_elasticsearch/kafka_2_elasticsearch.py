# -*- coding: utf-8 -*-

import pydash as _
import arrow
from time import sleep

from common.kafka import consumer
from common.log import logger
from common.mongo.bson_c import json_util

from .base import Base


class Kafka2Elasticsearch(Base):
    def __init__(self):
        Base.__init__(self)
        self._consumer = consumer.Consumer(topics=self.queue_key, group_id='oplog', enable_auto_commit=False,
                                           auto_offset_reset='earliest')

    def bulk(self):
        while True:
            try:
                count = 0
                start_time = arrow.utcnow()
                coll = self._consumer.poll(max_records=self.bulk_size)
                if coll and len(coll) > 0:
                    for messages in coll.values():
                        for record in messages:
                            try:
                                r = json_util.loads(record.value)
                                self.data_handlers[_.get(r, 'op')](r)
                                count += 1
                            except Exception as e:
                                self.event_emitter.emit('error', e)

                    self.generate_bulk_body()
                    self.do_task()

                    # commit
                    self._consumer.commit()

                logger.info('==== bulk indexed:{0} ===='.format(count))

                used_time = (arrow.utcnow() - start_time).seconds
                if used_time < self.wait:
                    sleep(self.wait - used_time)
            except Exception as e:
                self.event_emitter.emit('error', e)
                self._consumer.commit()

    def clear(self):
        for i in range(0, 1):
            self._consumer.poll(max_records=1)
        partitions = self._consumer.assignment()
        if partitions:
            print('end_offsets: ')
            print(self._consumer.end_offsets(partitions))
            self._consumer.seek_to_end()
            for partition in self._consumer.assignment():
                print('position: ')
                print(self._consumer.position(partition))
            self._consumer.commit()


q2e = Kafka2Elasticsearch()
