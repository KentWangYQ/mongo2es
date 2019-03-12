# -*- coding: utf-8 -*-

import time
import pymongo
import bson
from common.event_emitter import EventEmitter


class OplogEmitter(object):
    MONGO_URI = 'mongodb://127.0.0.1:27017/'

    def __init__(self, uri=None, **options):
        """初始化 OplogEmitter

        :param uri:
        :param options:
                ts: 8字节的时间戳，由4字节unix timestamp + 4字节自增计数表示。
                op：1字节的操作类型
                    "i"： insert
                    "u"： update
                    "d"： delete
                    "c"： db cmd
                    "n": no op,即空操作，其会定期执行以确保时效性
                ns：操作所在的namespace
                o：操作所对应的document，即当前操作的内容（比如更新操作时要更新的的字段和值）
                o2: 在执行更新操作时的where条件，仅限于update时才有该属性

        """
        self._close = False
        self._event_emitter = EventEmitter()
        self._client = pymongo.MongoClient((uri or self.MONGO_URI))
        self._oplog = self._client.local.oplog.rs
        self._query = self._generate_query(**options)

    def tail(self):
        while not self._close:
            cursor = self._oplog.find(self._query,
                                      cursor_type=pymongo.CursorType.TAILABLE,
                                      no_cursor_timeout=True,
                                      oplog_replay=True,
                                      batch_size=100)
            while not self._close and cursor.alive:
                for doc in cursor:
                    try:
                        # 触发data事件
                        self._event_emitter.emit('data', doc)

                        # 触发相应option事件
                        method_name = '_OplogEmitter_option_' + str(doc.get('op'))
                        method = getattr(self, method_name, lambda d: 'nothing')
                        method(doc)

                        # 触发相应option_collection事件
                        method_name = '_OplogEmitter_option_c_' + str(doc.get('op'))
                        method = getattr(self, method_name, lambda d: 'nothing')
                        method(doc)

                        self._query['ts'] = doc['ts']
                    except Exception as e:
                        self._event_emitter.emit('error', e, doc)

                time.sleep(1)

            cursor.close()

    def close(self):
        self._close = True

    def _option_i(self, data):
        self._event_emitter.emit('insert', data)

    def _option_u(self, data):
        self._event_emitter.emit('update', data)

    def _option_d(self, data):
        self._event_emitter.emit('delete', data)

    def _option_c(self, data):
        self._event_emitter.emit('cmd', data)

    def _option_n(self, data):
        self._event_emitter.emit('noop', data)

    def _option_c_i(self, data):
        self._event_emitter.emit(self._get_ns_collection(data['ns']) + '_insert', data)

    def _option_c_u(self, data):
        self._event_emitter.emit(self._get_ns_collection(data['ns']) + '_update', data)

    def _option_c_d(self, data):
        self._event_emitter.emit(self._get_ns_collection(data['ns']) + '_delete', data)

    @staticmethod
    def _get_ns_collection(ns):
        ns_l = ns.split('.')
        ns_l.pop(0)
        if len(ns_l) == 1:
            return ns_l[0]
        else:
            return '.'.join(ns_l)

    # tools
    @staticmethod
    def _generate_query(**options):
        q = {}

        ts = options.get('ts')

        ns = options.get('ns')
        include_ns = options.get('include_ns')
        exclude_ns = options.get('exclude_ns')

        op = options.get('op')
        include_ops = options.get('include_ops')
        exclude_ops = options.get('exclude_ops')

        if isinstance(ts, bson.Timestamp):
            q['ts'] = {'$gt': ts}

        # ns
        q_ns = []
        if isinstance(ns, str):
            if ns.endswith('.*'):
                q_ns.append({'ns': {'$regex': '^{0}\\.'.format(ns.replace('.*', ''))}})
        if isinstance(include_ns, list):
            q_ns.append({'ns': {'$in': include_ns}})
        if isinstance(exclude_ns, list):
            q_ns.append({'ns': {'$nin': exclude_ns}})
        if len(q_ns) > 0:
            q['$and'] = q_ns

        # op
        if isinstance(op, str):
            q['op'] = op
        elif isinstance(include_ops, list):
            q['op'] = {'$in': include_ops}
        elif isinstance(exclude_ops, list):
            q['op'] = {'$nin': exclude_ops}

        return q
