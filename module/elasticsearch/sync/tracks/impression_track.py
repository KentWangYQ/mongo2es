# -*- coding: utf-8 -*-

import pydash as _

from bson import objectid

from common import event_emitter
from common.log import logger
from common.elasticsearch.elasticsearch_client.elasticsearch_client import es_client
from common.elasticsearch import elasticsearch_util

from model import impression_track

from module.elasticsearch import util
from module.elasticsearch.sync import es_sync_util
from module.elasticsearch.const import structure
from module.elasticsearch.cursor import Cursor

from . import track_util

impression_track_cursor = Cursor(limit=2000,
                                 # filter={'_id': objectid.ObjectId('5a269e071fcd1025527cc53f')},
                                 projection={'update_time': 0})

opt = {
    'index': structure.tracks['index'],
    'type': structure.tracks['type']['impression_track'],
    'mappings': structure.tracks['mappings'],
    'settings': structure.tracks['settings'],
    'params': {
        'routing': structure.tracks['routing']
    }
}


def index():
    es_sync_util.index_exists_create(index=opt['index'], mappings=opt['mappings'], settings=opt['settings'])

    body = []
    index = 0

    course = impression_track.find(filter=impression_track_cursor.filter, projection=impression_track_cursor.projection,
                                   batch_size=impression_track_cursor.limit)

    impression_track_cursor.total = course.count()

    for item in course:
        body.append({'index': {'_id': str(item.pop('_id'))}})
        body.append(_.assign(item, track_util.url_split(_.get(item, 'uri'))))

        index += 1

        if (index % impression_track_cursor.limit) == 0 or index >= impression_track_cursor.total:
            res = es_client.bulk(index=opt['index'], doc_type=opt['type'], params=opt['params'], body=body)

            if res['errors']:
                raise elasticsearch_util.bulk_error_2_elasticsearch_exception(res['items'])
            else:
                impression_track_cursor.count += len(res['items'])
                logger.info('tracks impression_track indexed:{0}'.format(impression_track_cursor.count))

            body = []


def rt_index(mongo_oplog):
    @event_emitter.on(mongo_oplog.event_emitter, 'impressiontracks_insert')
    def on_insert(data):
        _id, obj = util.obj_from_oplog(data, impression_track_cursor.filter)

        if _id and obj:
            obj = util.dict_projection(obj, impression_track_cursor.projection)
            obj = _.assign(obj, track_util.url_split(_.get(obj, 'uri')))
            es_client.index(index=opt['index'], doc_type=opt['type'], id=_id, params=opt['params'],
                            body=obj)
