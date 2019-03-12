# -*- coding: utf-8 -*-

import pydash as _


from common.log import logger
from common.elasticsearch.elasticsearch_client.elasticsearch_client import es_client
from common.elasticsearch import elasticsearch_util

from model import act_share_detail

from module.elasticsearch import util
from module.elasticsearch.sync import es_sync_util
from module.elasticsearch.const import structure
from module.elasticsearch.cursor import Cursor

from . import track_util

act_share_detail_cursor = Cursor(limit=1000, projection={'createTime': 1, 'url': 1})

opt = {
    'index': structure.tracks['index'],
    'type': structure.tracks['type']['act_share_detail'],
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

    course = act_share_detail.find(filter=act_share_detail_cursor.filter, projection=act_share_detail_cursor.projection,
                                   batch_size=act_share_detail_cursor.limit)

    act_share_detail_cursor.total = course.count()

    for item in course:
        body.append({'index': {'_id': str(item.pop('_id'))}})
        body.append(_.assign(item, track_util.url_split(_.get(item, 'url'))))

        index += 1

        if (index % act_share_detail_cursor.limit) == 0 or index >= act_share_detail_cursor.total:
            res = es_client.bulk(index=opt['index'], doc_type=opt['type'], params=opt['params'], body=body)

            if res['errors']:
                raise elasticsearch_util.bulk_error_2_elasticsearch_exception(res['items'])
            else:
                act_share_detail_cursor.count += len(res['items'])
                logger.info('tracks act_share_detail indexed:{0}'.format(act_share_detail_cursor.count))

            body = []


def rt_index(mongo_oplog):
    @mongo_oplog.on('actsharedetails_insert')
    def on_insert(data):
        _id, obj = util.obj_from_oplog(data, act_share_detail_cursor.filter)

        if _id and obj:
            obj = util.dict_projection(obj, act_share_detail_cursor.projection)
            obj = _.assign(obj, track_util.url_split(_.get(obj, 'url')))
            es_client.index(index=opt['index'], doc_type=opt['type'], id=_id, params=opt['params'],
                            body=obj)
