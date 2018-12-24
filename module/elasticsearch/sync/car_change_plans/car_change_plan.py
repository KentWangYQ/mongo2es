# -*- coding: utf-8 -*-

import pydash as _
from bson import objectid

from common import event_emitter
from common.log import logger

from model import car_change_plan

from common.elasticsearch.elasticsearch_client.elasticsearch_client import es_client
from common.elasticsearch import elasticsearch_util

from module.elasticsearch import util
from module.elasticsearch.sync import es_sync_util
from module.elasticsearch.const import structure
from module.elasticsearch.cursor import Cursor

car_product_cursor = Cursor(filter={'stop_sale': False, 'model.modelId': {'$nin': [None, '']}},
                            projection={'che300Model': 0, 'che300Parameters': 0, 'che168Id': 0,
                                        'che168ParamTypeItems': 0, 'che168ConfigTypeItems': 0})
city_cursor = Cursor(projection={'areas': 0})
owner_cursor = Cursor(projection={'username': 1, 'realName': 1, 'phone': 1, 'cityId': 1, 'cityName': 1})
car_keeper_cursor = owner_cursor

car_change_plan_cursor = Cursor(limit=1000,
                                projection={'loan': 0,
                                            'intent_ref.extra_data.car_change_plan.loan': 0},
                                pop_fields={
                                    'new_car': {
                                        'from': 'carproducts',
                                        'local_field': 'new_car.car_products_id',
                                        'foreign_field': '_id',
                                        'as': 'new_car_info',
                                        'projection': car_product_cursor.projection
                                    },
                                    'city': {
                                        'from': 'cities',
                                        'local_field': 'city_id',
                                        'foreign_field': '_id',
                                        'as': 'city',
                                        'projection': city_cursor.projection
                                    },
                                    'owner': {
                                        'from': 'users',
                                        'local_field': 'owner',
                                        'foreign_field': '_id',
                                        'as': 'owner',
                                        'projection': owner_cursor.projection
                                    },
                                    'car_keeper': {
                                        'from': 'users',
                                        'local_field': 'carkeeper',
                                        'foreign_field': '_id',
                                        'as': 'car_keeper',
                                        'projection': car_keeper_cursor.projection
                                    }
                                })

opt = {
    'index': structure.car_change_plans['index'],
    'type': structure.car_change_plans['type']['car_change_plan'],
    'mappings': structure.car_change_plans['mappings'],
    'settings': structure.car_change_plans['settings'],
    'params': {
        'routing': structure.car_change_plans['routing']
    }
}


def index():
    body = []
    index = 0

    course = car_change_plan.populates(filter=car_change_plan_cursor.filter,
                                       projection=car_change_plan_cursor.projection,
                                       pop_fields=car_change_plan_cursor.pop_fields)

    car_change_plan_cursor.total = len(course)

    for item in course:
        body.append({
            'index': {
                '_id': str(item.pop('_id')),
                '_parent': str(_.get(item, 'used_car.used_car_id'))
            }
        })
        body.append(item)

        index += 1

        if (index % car_change_plan_cursor.limit) == 0 or index >= car_change_plan_cursor.total:
            res = es_client.bulk(index=opt['index'], doc_type=opt['type'], params=opt['params'], body=body)

            if res['errors']:
                raise elasticsearch_util.bulk_error_2_elasticsearch_exception(res['items'])
            else:
                car_change_plan_cursor.count += len(res['items'])
                logger.info('carChangePlan car_change_plan indexed:{0}'.format(car_change_plan_cursor.count))

            body = []


def index_one(_id):
    item = car_change_plan.populate_one(
        filter=dict({'_id': objectid.ObjectId(_id)}, **(car_change_plan_cursor.filter or {})),
        projection=car_change_plan_cursor.projection,
        pop_fields=car_change_plan_cursor.pop_fields)
    if item:
        es_client.index(index=opt['index'], doc_type=opt['type'], id=str(item.pop('_id')),
                        parent=str(_.get(item, 'used_car.used_car_id')), params=opt['params'],
                        body=item)


def rt_index(mongo_oplog):
    @event_emitter.on(mongo_oplog.event_emitter, 'carchangeplans_insert')
    def on_insert(data):
        _id, obj = util.obj_from_oplog(data, car_change_plan_cursor.filter)

        if _id and obj:
            index_one(_id)

    @event_emitter.on(mongo_oplog.event_emitter, 'carchangeplans_update')
    def on_update(data):
        _id, obj = util.obj_from_oplog(data, car_change_plan_cursor.filter)

        if _id and obj:
            index_one(_id)
        else:
            es_sync_util.delete(index=opt['index'], doc_type=opt['type'], data=data,
                                _filter=car_change_plan_cursor.filter,
                                es_params=opt['params'])

    @event_emitter.on(mongo_oplog.event_emitter, 'carchangeplans_delete')
    def on_delete(data):
        es_sync_util.delete(index=opt['index'], doc_type=opt['type'], data=data, _filter=car_change_plan_cursor.filter,
                            es_params=opt['params'])

    @event_emitter.on(mongo_oplog.event_emitter, 'carproducts_update')
    def on_carproduct_update(data):
        es_sync_util.update_by_query(index=opt['index'], doc_type=opt['type'], data=data,
                                     projection=car_product_cursor.projection, _filter=car_product_cursor.filter,
                                     _as=car_change_plan_cursor.pop_fields.get('new_car').get('as'))

    @event_emitter.on(mongo_oplog.event_emitter, 'carproducts_delete')
    def on_carproduct_delete(data):
        es_sync_util.update_by_query(index=opt['index'], doc_type=opt['type'], data=data,
                                     projection=car_product_cursor.projection, _filter=car_product_cursor.filter,
                                     _as=car_change_plan_cursor.pop_fields.get('new_car').get('as'))

    @event_emitter.on(mongo_oplog.event_emitter, 'cities_update')
    def on_city_update(data):
        es_sync_util.update_by_query(index=opt['index'], doc_type=opt['type'], data=data,
                                     projection=city_cursor.projection, _filter=city_cursor.filter,
                                     _as=car_change_plan_cursor.pop_fields.get('city').get('as'))

    @event_emitter.on(mongo_oplog.event_emitter, 'cities_update')
    def on_city_delete(data):
        es_sync_util.update_by_query(index=opt['index'], doc_type=opt['type'], data=data,
                                     projection=car_product_cursor.projection, _filter=car_product_cursor.filter,
                                     _as=car_change_plan_cursor.pop_fields.get('city').get('as'))

    @event_emitter.on(mongo_oplog.event_emitter, 'users_update')
    def on_owner_update(data):
        es_sync_util.update_by_query(index=opt['index'], doc_type=opt['type'], data=data,
                                     projection=owner_cursor.projection, _filter=owner_cursor.filter,
                                     _as=car_change_plan_cursor.pop_fields.get('owner').get('as'))

    @event_emitter.on(mongo_oplog.event_emitter, 'users_update')
    def on_owner_delete(data):
        es_sync_util.update_by_query(index=opt['index'], doc_type=opt['type'], data=data,
                                     projection=owner_cursor.projection, _filter=owner_cursor.filter,
                                     _as=car_change_plan_cursor.pop_fields.get('owner').get('as'))

    @event_emitter.on(mongo_oplog.event_emitter, 'users_update')
    def on_car_keeper_update(data):
        es_sync_util.update_by_query(index=opt['index'], doc_type=opt['type'], data=data,
                                     projection=car_keeper_cursor.projection, _filter=car_keeper_cursor.filter,
                                     _as=car_change_plan_cursor.pop_fields.get('car_keeper').get('as'))

    @event_emitter.on(mongo_oplog.event_emitter, 'users_update')
    def on_car_keeper_delete(data):
        es_sync_util.update_by_query(index=opt['index'], doc_type=opt['type'], data=data,
                                     projection=car_keeper_cursor.projection, _filter=car_keeper_cursor.filter,
                                     _as=car_change_plan_cursor.pop_fields.get('car_keeper').get('as'))
