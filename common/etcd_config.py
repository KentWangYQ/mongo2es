# -*- coding: utf-8 -*-

import etcd

from config import settings

client = etcd.Client(host=settings.ETCD.get('hosts'),
                     username=settings.ETCD.get('options').get('auth').get('users'),
                     password=settings.ETCD.get('options').get('auth').get('pass'),
                     allow_reconnect=True)


def get(key):
    try:
        return client.get(key)
    except etcd.EtcdKeyNotFound:
        return None


class ETCD_Client(object):
    def __init__(self, key):
        self.key = key if key[-1:] == '/' else key + '/'

    def get(self, key):
        return self.__mapping(self.key).get(key)

    def set(self, key, value):
        return client.set(self.key + key, value)

    def delete(self, key, recursive, dir, **kwargs):
        key = self.key + key
        return client.delete(key=key, recursive=recursive, dir=dir, **kwargs)

    @staticmethod
    def __mapping(key):
        obj = get(key)
        if obj is not None:
            return {k.split('/')[-1]: v for k, v in (obj._children or {}).items()}
            # for item in obj._children:
            #     result[item['key'].split('/')[-1]] = item['value']
        return {}


mongo2es = ETCD_Client(settings.ETCD.get('keys').get('MONGO2ES'))
