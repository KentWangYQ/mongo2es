# -*- coding: utf-8 -*-

from common import event_emitter
from common.log import logger
from common.elasticsearch.elasticsearch_client.elasticsearch_client import es_client
from common.elasticsearch import elasticsearch_util

from model import order

from module.elasticsearch import util
from module.elasticsearch.sync import es_sync_util
from module.elasticsearch.const import structure
from module.elasticsearch.cursor import Cursor

order_cursor = Cursor(limit=1000, filter={}, projection={'brokerage': 0})

opt = {
    'index': structure.car_change_plans['index'],
    'type': structure.car_change_plans['type']['order'],
    'mappings': structure.car_change_plans['mappings'],
    'settings': structure.car_change_plans['settings'],
    'params': {
        'routing': structure.car_change_plans['routing']
    }
}


def index():
    body = []
    index = 0

    course = order.find(filter=order_cursor.filter, projection=order_cursor.projection,
                        batch_size=order_cursor.limit)

    order_cursor.total = course.count()

    for item in course:
        body.append({
            'index': {
                '_id': str(item.pop('_id')),
                '_parent': str(item.get('car_change_plan_id'))
            }
        })
        body.append(item)

        index += 1

        if (index % order_cursor.limit) == 0 or index >= order_cursor.total:
            res = es_client.bulk(index=opt['index'], doc_type=opt['type'], params=opt['params'], body=body)

            if res['errors']:
                raise elasticsearch_util.bulk_error_2_elasticsearch_exception(res['items'])
            else:
                order_cursor.count += len(res['items'])
                logger.info('carChangePlan order indexed:{0}'.format(order_cursor.count))

            body = []


def rt_index(mongo_oplog):
    @event_emitter.on(mongo_oplog.event_emitter, 'orders_insert')
    def on_insert(data):
        _id, obj = util.obj_from_oplog(data, order_cursor.filter)

        if _id and obj:
            parent = obj.get('car_change_plan_id')
            obj = util.dict_projection(obj, order_cursor.projection)

            es_client.index(index=opt['index'], doc_type=opt['type'], id=_id, parent=parent, params=opt['params'],
                            body=obj)

    @event_emitter.on(mongo_oplog.event_emitter, 'orders_update')
    def on_update(data):
        es_sync_util.update_by_query(index=opt['index'], doc_type=opt['type'], data=data,
                                     projection=order_cursor.projection, _filter=order_cursor.filter)

    @event_emitter.on(mongo_oplog.event_emitter, 'orders_delete')
    def on_delete(data):
        es_sync_util.delete(index=opt['index'], doc_type=opt['type'], data=data, _filter=order_cursor.filter,
                            es_params=opt['params'])
