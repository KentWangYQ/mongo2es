# -*- coding: utf-8 -*-

import itertools

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
    course = order.find(filter=order_cursor.filter, projection=order_cursor.projection,
                        batch_size=order_cursor.limit)

    def gen_body(_course):
        result = None
        while result is None or len(result) == order_cursor.limit:
            result = [
                [{'index': {'_id': str(v.pop('_id'))}}, v]
                for v in itertools.islice(_course, order_cursor.limit)
            ]
            if len(result) > 0:
                yield _.flatten(result)

    for body in gen_body(course):
        res = es_client.bulk(index=opt['index'], doc_type=opt['type'], params=opt['params'], body=body)

        if res['errors']:
            raise elasticsearch_util.bulk_error_2_elasticsearch_exception(res['items'])
        else:
            order_cursor.count += len(res['items'])
            logger.info('activity order indexed:{0}'.format(order_cursor.count))


def rt_index(mongo_oplog):
    @mongo_oplog.on('orders_insert')
    def on_insert(data):
        _id, obj = util.obj_from_oplog(data, order_cursor.filter)

        if _id and obj:
            parent = obj.get('car_change_plan_id')
            obj = util.dict_projection(obj, order_cursor.projection)

            es_client.index(index=opt['index'], doc_type=opt['type'], id=_id, parent=parent, params=opt['params'],
                            body=obj)

    @mongo_oplog.on('orders_update')
    def on_update(data):
        es_sync_util.update_by_query(index=opt['index'], doc_type=opt['type'], data=data,
                                     projection=order_cursor.projection, _filter=order_cursor.filter)

    @mongo_oplog.on('orders_delete')
    def on_delete(data):
        es_sync_util.delete(index=opt['index'], doc_type=opt['type'], data=data, _filter=order_cursor.filter,
                            es_params=opt['params'])
