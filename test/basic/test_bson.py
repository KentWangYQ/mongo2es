import unittest
# from bson import json_util
from common.mongo.bson_c import json_util


class BsonTest(unittest.TestCase):
    def test_json_util_dumps(self):
        r = json_util.dumps({'test': True})
        print(r)
