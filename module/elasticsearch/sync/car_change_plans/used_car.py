# -*- coding: utf-8 -*-

from bson import objectid

from common import event_emitter

from common.log import logger

from model import used_car
from common.elasticsearch.elasticsearch_client.elasticsearch_client import es_client
from common.elasticsearch import elasticsearch_util
from module.elasticsearch import util
from module.elasticsearch.const import structure
from module.elasticsearch.cursor import Cursor
from module.elasticsearch.sync import es_sync_util

owner_cursor = Cursor(projection={'username': 1, 'realName': 1, 'phone': 1, 'cityId': 1, 'cityName': 1})

used_car_cursor = Cursor(limit=1000,
                         pop_fields={
                             'owner': {
                                 'from': 'users',
                                 'local_field': 'owner',
                                 'foreign_field': '_id',
                                 'as': 'owner',
                                 'projection': owner_cursor.projection
                             }
                         })

opt = {
    'index': structure.car_change_plans['index'],
    'type': structure.car_change_plans['type']['used_car'],
    'mappings': structure.car_change_plans['mappings'],
    'settings': structure.car_change_plans['settings'],
    'params': {
        'routing': structure.car_change_plans['routing']
    }
}


def index():
    es_sync_util.index_exists_create(index=opt['index'], mappings=opt['mappings'], settings=opt['settings'])

    body = []
    index = 0

    course = used_car.populates(
        filter=used_car_cursor.filter,
        projection=used_car_cursor.projection,
        pop_fields=used_car_cursor.pop_fields)

    used_car_cursor.total = len(course)

    for item in course:
        body.append({'index': {'_id': str(item.pop('_id'))}})
        body.append(item)

        index += 1

        if (index % used_car_cursor.limit) == 0 or index >= used_car_cursor.total:
            res = es_client.bulk(index=opt['index'], doc_type=opt['type'], params=opt['params'], body=body)

            if res['errors']:
                raise elasticsearch_util.bulk_error_2_elasticsearch_exception(res['items'])
            else:
                used_car_cursor.count += len(res['items'])
                logger.info('carChangePlan used_car indexed:{0}'.format(used_car_cursor.count))

            body = []


def index_one(_id):
    item = used_car.populate_one(
        filter=dict({'_id': objectid.ObjectId(_id)}, **(used_car_cursor.filter or {})),
        projection=used_car_cursor.projection,
        pop_fields=used_car_cursor.pop_fields)

    if item:
        es_client.index(index=opt['index'], doc_type=opt['type'], id=str(item.pop('_id')), params=opt['params'],
                        body=item)


def rt_index(mongo_oplog):
    @event_emitter.on(mongo_oplog.event_emitter, 'usedcars_insert')
    def on_insert(data):
        _id, obj = util.obj_from_oplog(data, used_car_cursor.filter)

        if _id and obj:
            index_one(_id)

    @event_emitter.on(mongo_oplog.event_emitter, 'usedcars_update')
    def on_update(data):
        _id, obj = util.obj_from_oplog(data, used_car_cursor.filter)

        if _id and obj:
            index_one(_id)
        else:
            es_sync_util.delete(index=opt['index'], doc_type=opt['type'], data=data, _filter=used_car_cursor.filter,
                                es_params=opt['params'])

    @event_emitter.on(mongo_oplog.event_emitter, 'usedcars_delete')
    def on_delete(data):
        es_sync_util.delete(index=opt['index'], doc_type=opt['type'], data=data, _filter=used_car_cursor.filter,
                            es_params=opt['params'])

    @event_emitter.on(mongo_oplog.event_emitter, 'users_update')
    def on_owner_update(data):
        es_sync_util.update_by_query(index=opt['index'], doc_type=opt['type'], data=data,
                                     projection=owner_cursor.projection, _filter=owner_cursor.filter,
                                     _as=used_car_cursor.pop_fields.get('owner').get('as'))

    @event_emitter.on(mongo_oplog.event_emitter, 'users_update')
    def on_owner_delete(data):
        es_sync_util.update_by_query(index=opt['index'], doc_type=opt['type'], data=data,
                                     projection=owner_cursor.projection, _filter=owner_cursor.filter,
                                     _as=used_car_cursor.pop_fields.get('owner').get('as'))
