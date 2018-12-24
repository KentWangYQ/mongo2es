from common.mongo.bson_c import json_util
from model import merchant

merchants = merchant.populates(filter={'support': {'$nin': [None, '']}},
                               projection={'_id': 1, 'name': 1, 'sname': 1, 'support': 1},
                               sort=[('_id', 1)],
                               skip=0,
                               limit=10,
                               pop_fields={
                                   'user': {
                                       'from': 'users',
                                       'local_field': 'support',
                                       'foreign_field': '_id',
                                       'projection': {'_id': 1, 'phone': 1},
                                       'pop_fields': {
                                           'city': {
                                               'from': 'cities',
                                               'local_field': 'cityId',
                                               'foreign_field': '_id',
                                               'projection': {'_id': 1, 'name': 1},
                                               'as': 'city'
                                           }
                                       }
                                   }
                               })

for m in merchants:
    print(json_util.dumps(m))
