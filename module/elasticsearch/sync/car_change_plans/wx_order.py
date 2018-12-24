# -*- coding: utf-8 -*-

import pydash as _

from common import event_emitter
from common.log import logger
from common.elasticsearch.elasticsearch_client.elasticsearch_client import es_client
from common.elasticsearch import elasticsearch_util

from model import wx_order

from module.elasticsearch import util
from module.elasticsearch.sync import es_sync_util
from module.elasticsearch.const import structure
from module.elasticsearch.cursor import Cursor

wx_order_cursor = Cursor(limit=1000, pop_fields={
    'order': {
        'from': 'orders',
        'local_field': '_id',
        'foreign_field': 'wx_order_id',
        'as': 'order',
        'projection': {'_id':1,'wx_order_id':1}
    }
})

opt = {
    'index': structure.car_change_plans['index'],
    'type': structure.car_change_plans['type']['wx_order'],
    'mappings': structure.car_change_plans['mappings'],
    'settings': structure.car_change_plans['settings'],
    'params': {
        'routing': structure.car_change_plans['routing']
    }
}


def index():

    body = []
    index = 0

    course = wx_order.populates(filter=wx_order_cursor.filter,
                                pop_fields=wx_order_cursor.pop_fields,
                                field_value_filter=lambda v:str(v)
                                )

    wx_order_cursor.total = len(course)

    for item in course:
        order = item.pop('order')
        body.append({
            'index': {
                '_id': str(item.pop('_id')),
                '_parent': _.get(order,'_id')
            }
        })

        body.append(item)

        index += 1
        if (index % wx_order_cursor.limit) == 0 or index >= wx_order_cursor.total:

            res = es_client.bulk(
                index=opt['index'], doc_type=opt['type'], params=opt['params'], body=body)

            if res['errors']:
                raise elasticsearch_util.bulk_error_2_elasticsearch_exception(
                    res['items'])
            else:
                wx_order_cursor.count += len(res['items'])
                logger.info('carChangePlan order wxorder indexed:{0}'.format(
                    wx_order_cursor.count))

            body = []

def rt_index(mongo_oplog):
    @event_emitter.on(mongo_oplog.event_emitter, 'wxorders_insert')
    def on_insert(data):
        _id, obj = util.obj_from_oplog(data,wx_order_cursor.filter, pop_fields=wx_order_cursor.pop_fields)
        if _id and obj:
            parent = _.get(obj,'order')
            obj = util.dict_projection(obj, wx_order_cursor.projection)
            if parent:
                es_client.index(index=opt['index'], doc_type=opt['type'], id=_id, parent=parent, params=opt['params'],
                                body=obj)
            else:
                es_client.index(index=opt['index'], doc_type=opt['type'], id=_id, params=opt['params'],
                            body=obj)

    @event_emitter.on(mongo_oplog.event_emitter, 'wxorders_update')
    def on_update(data):
        es_sync_util.update_by_query(index=opt['index'], doc_type=opt['type'], data=data,
                                     projection=wx_order_cursor.projection, _filter=wx_order_cursor.filter)

    @event_emitter.on(mongo_oplog.event_emitter, 'wxorders_delete')
    def on_delete(data):
        es_sync_util.delete(index=opt['index'], doc_type=opt['type'], data=data, _filter=wx_order_cursor.filter,
                            es_params=opt['params'])
