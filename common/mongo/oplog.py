# -*- coding: utf-8 -*-

import time

import pymongo

from . import filter
from common.event_emitter import EventEmitter


class MongoOplog(EventEmitter):
    _uri = 'mongodb://127.0.0.1:27017/'

    __close = False

    def __init__(self, uri=None, **options):
        """初始化MongoOplog

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
        EventEmitter.__init__(self)
        self.__close = False

        self._client = pymongo.MongoClient((uri or self._uri))
        self._oplog = self._client.local.oplog.rs
        self._query = filter.generate_query(**options)

    def tail(self):
        while not self.__close:
            cursor = self._oplog.find(self._query,
                                      cursor_type=pymongo.CursorType.TAILABLE,
                                      no_cursor_timeout=True,
                                      oplog_replay=True,
                                      batch_size=100)
            while not self.__close and cursor.alive:
                for doc in cursor:
                    try:
                        # 触发data事件
                        self.emit('data', doc)

                        # 触发相应option事件
                        method_name = '_MongoOplog__option_' + str(doc.get('op'))
                        method = getattr(self, method_name, lambda d: 'nothing')
                        method(doc)

                        # 触发相应option_collection事件
                        method_name = '_MongoOplog__option_c_' + str(doc.get('op'))
                        method = getattr(self, method_name, lambda d: 'nothing')
                        method(doc)

                        self._query['ts'] = doc['ts']
                    except Exception as e:
                        self.emit('error', e, doc)

                time.sleep(1)

            cursor.close()

    def close(self):
        self.__close = True

    def __option_i(self, data):
        self.emit('insert', data)

    def __option_u(self, data):
        self.emit('update', data)

    def __option_d(self, data):
        self.emit('delete', data)

    def __option_c(self, data):
        self.emit('cmd', data)

    def __option_n(self, data):
        self.emit('noop', data)

    def __option_c_i(self, data):
        self.emit(self.__get_ns_collection(data['ns']) + '_insert', data)

    def __option_c_u(self, data):
        self.emit(self.__get_ns_collection(data['ns']) + '_update', data)

    def __option_c_d(self, data):
        self.emit(self.__get_ns_collection(data['ns']) + '_delete', data)

    @staticmethod
    def __get_ns_collection(ns):
        ns_l = ns.split('.')
        ns_l.pop(0)
        if len(ns_l) == 1:
            return ns_l[0]
        else:
            return '.'.join(ns_l)
