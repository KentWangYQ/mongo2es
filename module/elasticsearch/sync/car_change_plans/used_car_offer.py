# -*- coding: utf-8 -*-

import pydash as _
from bson import objectid


from common.log import logger
from common.elasticsearch.elasticsearch_client.elasticsearch_client import es_client
from common.elasticsearch import elasticsearch_util

from model import used_car_offer
from module.elasticsearch import util
from module.elasticsearch.sync import es_sync_util
from module.elasticsearch.const import structure
from module.elasticsearch.cursor import Cursor

merchant_cursor = Cursor(limit=20,
                         filter={'brand': {'$nin': [None, []]}},
                         projection={'name': 1, 'sname': 1, 'brand': 1, 'city': 1, 'support': 1},
                         pop_fields={
                             'support': {
                                 'from': 'users',
                                 'local_field': 'support',
                                 'foreign_field': '_id',
                                 'projection': {'_id': 1, 'username': 1, 'realName': 1, 'cityName': 1,
                                                'cityId': 1, 'zone': 1, 'phone': 1, 'status': 1}
                             }
                         })

used_car_offer_cursor = Cursor(limit=1000,
                               pop_fields={
                                   'merchant': {
                                       'from': 'merchants',
                                       'local_field': 'merchant',
                                       'foreign_field': '_id',
                                       'as': 'merchant',
                                       'projection': merchant_cursor.projection,
                                       'pop_fields': merchant_cursor.pop_fields
                                   },
                                   'appraiser': {
                                       'from': 'users',
                                       'local_field': 'appraiser',
                                       'foreign_field': '_id',
                                       'projection': {'_id': 1, 'username': 1, 'realName': 1, 'cityName': 1,
                                                      'cityId': 1, 'zone': 1, 'phone': 1, 'status': 1}
                                   }
                               })

opt = {
    'index': structure.car_change_plans['index'],
    'type': structure.car_change_plans['type']['used_car_offer'],
    'mappings': structure.car_change_plans['mappings'],
    'settings': structure.car_change_plans['settings'],
    'params': {
        'routing': structure.car_change_plans['routing']
    }
}


def index():
    body = []
    index = 0

    course = used_car_offer.populates(filter=used_car_offer_cursor.filter, projection=used_car_offer_cursor.projection,
                                      pop_fields=used_car_offer_cursor.pop_fields)

    used_car_offer_cursor.total = len(course)

    for item in course:
        body.append({
            'index': {
                '_id': str(item.pop('_id')),
                '_parent': str(item.get('used_car_id'))
            }
        })
        body.append(item)

        index += 1

        if (index % used_car_offer_cursor.limit) == 0 or index >= used_car_offer_cursor.total:
            res = es_client.bulk(index=opt['index'], doc_type=opt['type'], params=opt['params'], body=body)

            if res['errors']:
                raise elasticsearch_util.bulk_error_2_elasticsearch_exception(res['items'])
            else:
                used_car_offer_cursor.count += len(res['items'])
                logger.info('carChangePlan used_car_offer indexed:{0}'.format(used_car_offer_cursor.count))

            body = []


def index_one(_id):
    item = used_car_offer.populate_one(
        filter=dict({'_id': objectid.ObjectId(_id)}, **(used_car_offer_cursor.filter or {})),
        projection=used_car_offer_cursor.projection,
        pop_fields=used_car_offer_cursor.pop_fields)
    if item:
        es_client.index(index=opt['index'], doc_type=opt['type'], id=str(item.pop('_id')),
                        parent=str(item.get('used_car_id')), params=opt['params'],
                        body=item)


def rt_index(mongo_oplog):
    @mongo_oplog.on('usedcaroffers_insert')
    def on_insert(data):
        _id, obj = util.obj_from_oplog(data, used_car_offer_cursor.filter)

        if _id and obj:
            index_one(_id)

    @mongo_oplog.on('usedcaroffers_update')
    def on_update(data):
        _id, obj = util.obj_from_oplog(data, used_car_offer_cursor.filter)

        if _id and obj:
            index_one(_id)
        else:
            es_sync_util.delete(index=opt['index'], doc_type=opt['type'], data=data,
                                _filter=used_car_offer_cursor.filter,
                                es_params=opt['params'])

    @mongo_oplog.on('usedcaroffers_delete')
    def on_delete(data):
        es_sync_util.delete(index=opt['index'], doc_type=opt['type'], data=data, _filter=used_car_offer_cursor.filter,
                            es_params=opt['params'])

    @mongo_oplog.on('merchants_update')
    def on_merchant_update(data):
        es_sync_util.update_by_query(index=opt['index'], doc_type=opt['type'], data=data,
                                     projection=merchant_cursor.projection,
                                     _filter=merchant_cursor.filter,
                                     pop_fields=merchant_cursor.pop_fields,
                                     _as=_.get(used_car_offer_cursor.pop_fields, 'merchant.as'))

    # populate - support信息更新
    @mongo_oplog.on('users_update')
    def on_support_update(data):
        _as = _.get(merchant_cursor.pop_fields, 'support.as') or _.get(merchant_cursor.pop_fields,
                                                                       'support.local_field')
        es_sync_util.update_by_query(index=opt['index'], doc_type=opt['type'], data=data,
                                     projection=_.get(merchant_cursor.pop_fields, 'support.projection'), _as=_as)

    # populate - support信息删除
    @mongo_oplog.on('users_delete')
    def on_support_delete(data):
        _as = _.get(merchant_cursor.pop_fields, 'support.as') or _.get(merchant_cursor.pop_fields,
                                                                       'support.local_field')
        es_sync_util.update_by_query(index=opt['index'], doc_type=opt['type'], data=data,
                                     projection=_.get(merchant_cursor.pop_fields, 'support.projection'), _as=_as)

    # populate - appraiser 信息更新
    @mongo_oplog.on('users_update')
    def on_support_update(data):
        _as = _.get(used_car_offer_cursor.pop_fields, 'appraiser.as') or _.get(used_car_offer_cursor.pop_fields,
                                                                               'appraiser.local_field')
        es_sync_util.update_by_query(index=opt['index'], doc_type=opt['type'], data=data,
                                     projection=_.get(used_car_offer_cursor.pop_fields, 'appraiser.projection'),
                                     _as=_as)

    # populate - appraiser 信息删除
    @mongo_oplog.on('users_delete')
    def on_support_delete(data):
        _as = _.get(used_car_offer_cursor.pop_fields, 'appraiser.as') or _.get(used_car_offer_cursor.pop_fields,
                                                                               'appraiser.local_field')
        es_sync_util.update_by_query(index=opt['index'], doc_type=opt['type'], data=data,
                                     projection=_.get(used_car_offer_cursor.pop_fields, 'appraiser.projection'),
                                     _as=_as)
