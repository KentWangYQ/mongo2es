# -*- coding: utf-8 -*-

import pydash as _
import copy

from config import settings

from common.log import logger
from common import event_emitter as c_event_emitter
from common.elasticsearch.elasticsearch_client.elasticsearch_client import ElasticsearchClient
from common.elasticsearch import elasticsearch_util

es_client = ElasticsearchClient('direct').client


class Base(object):

    @property
    def event_emitter(self):
        return self.__event_emitter

    @property
    def wait(self):
        return self.__wait

    @property
    def bulk_size(self):
        return self.__bulk_size

    @property
    def queue_key(self):
        return self.__queue_key

    @property
    def data_handlers(self):
        return self.__data_handlers

    @property
    def task_handlers(self):
        return self.__task_handlers

    def __init__(self):
        self.__event_emitter = c_event_emitter.EventEmitter()
        self.__wait = _.get(settings.SYNC, 'rts.queue.wait', 5)
        self.__bulk_size = _.get(settings.SYNC, 'rts.queue.bulk_size', 100)
        self.__queue_key = _.get(settings.SYNC, 'rts.queue.key', 'oplog')
        self.__data_handlers = {
            'create': self.data_process_create,
            'index': self.data_process_index,
            'update': self.data_process_update,
            'delete': self.data_process_delete,
            'bulk': self.data_process_bulk,
            'update_by_query': self.data_process_update_by_query,
            'delete_by_query': self.data_process_delete_by_query,
        }
        self.__task_handlers = {
            'create': self.task_process_create,
            'index': self.task_process_index,
            'update': self.task_process_update,
            'delete': self.task_process_delete,
            'bulk': self.task_process_bulk,
            'update_by_query': self.task_process_update_by_query,
            'delete_by_query': self.task_process_delete_by_query,
        }
        self.__body = []
        self.__task = []

    def bulk(self):
        raise NotImplementedError

    def do_task(self):
        for task in self.__task:
            try:
                self.__task_handlers[_.get(task, 'op')](task)
            except Exception as e:
                logger.error(e)
                self.__event_emitter.emit('error', e)

        self.__task = []

    def data_process_create(self, r):
        _index = _.get(r, 'index')
        _type = _.get(r, 'doc_type')
        _id = _.get(r, 'id')
        body = _.get(r, 'body')
        params = _.get(r, 'params') or {}

        self.__body.extend([
            {'create': dict({'_index': _index, '_type': _type, '_id': _id}, **params)},
            body
        ])

    def data_process_index(self, r):
        _index = _.get(r, 'index')
        _type = _.get(r, 'doc_type')
        _id = _.get(r, 'id')
        body = _.get(r, 'body')
        params = _.get(r, 'params') or {}

        self.__body.extend([
            {'index': dict({'_index': _index, '_type': _type, '_id': _id}, **params)},
            body
        ])

    def data_process_update(self, r):
        _index = _.get(r, 'index')
        _type = _.get(r, 'doc_type')
        _id = _.get(r, 'id')
        script = _.get(r, 'body.script')
        params = _.get(r, 'params') or {}

        self.__body.extend([
            {'update': dict({'_index': _index, '_type': _type, '_id': _id}, **params)},
            {'script': script}
        ])

    def data_process_delete(self, r):
        _index = _.get(r, 'index')
        _type = _.get(r, 'doc_type')
        _id = _.get(r, 'id')
        params = _.get(r, 'params') or {}

        self.__body.extend([
            {'delete': dict({'_index': _index, '_type': _type, '_id': _id}, **params)},
        ])

    def data_process_bulk(self, r):
        self.generate_bulk_body()

        self.__task.append(r)

    def data_process_update_by_query(self, r):
        self.generate_bulk_body()
        self.__task.append(r)

    def data_process_delete_by_query(self, r):
        self.generate_bulk_body()

        self.__task.append(r)

    def task_process_create(self, r):
        index = _.get(r, 'index')
        doc_type = _.get(r, 'doc_type')
        id = _.get(r, 'id')
        body = _.get(r, 'body')
        params = _.get(r, 'params') or {}

        return es_client.create(index=index, doc_type=doc_type, id=id, body=body, params=params)

    def task_process_index(self, r):
        index = _.get(r, 'index')
        doc_type = _.get(r, 'doc_type')
        id = _.get(r, 'id')
        body = _.get(r, 'body')
        params = _.get(r, 'params') or {}

        return es_client.index(index=index, doc_type=doc_type, id=id, body=body, params=params)

    def task_process_update(self, r):
        index = _.get(r, 'index')
        doc_type = _.get(r, 'doc_type')
        id = _.get(r, 'id')
        body = _.get(r, 'body')
        params = _.get(r, 'params') or {}

        return es_client.update(index=index, doc_type=doc_type, id=id, body=body, params=params)

    def task_process_delete(self, r):
        index = _.get(r, 'index')
        doc_type = _.get(r, 'doc_type')
        id = _.get(r, 'id')
        params = _.get(r, 'params') or {}

        return es_client.delete(index=index, doc_type=doc_type, id=id, params=params)

    def task_process_bulk(self, r):
        index = _.get(r, 'index')
        doc_type = _.get(r, 'doc_type')
        body = _.get(r, 'body')
        params = _.get(r, 'params') or {}

        result = es_client.bulk(body=body, index=index, doc_type=doc_type, params=params)
        if result['errors']:
            raise elasticsearch_util.bulk_error_2_elasticsearch_exception(result['items'])

        return result

    def task_process_update_by_query(self, r):
        index = _.get(r, 'index')
        doc_type = _.get(r, 'doc_type')
        body = _.get(r, 'body')
        params = _.get(r, 'params') or {}

        return es_client.update_by_query(index=index, doc_type=doc_type, body=body, params=params)

    def task_process_delete_by_query(self, r):
        index = _.get(r, 'index')
        doc_type = _.get(r, 'doc_type')
        body = _.get(r, 'body')
        params = _.get(r, 'params') or {}

        return es_client.delete_by_query(index=index, body=body, doc_type=doc_type, params=params)

    def generate_bulk_body(self):
        if len(self.__body) > 0:
            self.__task.append({'op': 'bulk', 'body': copy.deepcopy(self.__body)})
            self.__body = []

    def clear(self):
        raise NotImplementedError


q2e = Base()
