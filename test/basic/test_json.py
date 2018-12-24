import unittest
import json


class JsonTest(unittest.TestCase):
    def test_json_dumps(self):
        source = 'TypeError: ReadTimeoutError("HTTPConnectionPool(host="172.16.200.173", port=9200): Read timed out. (read timeout=10)",)'
        s = json.dumps(source)
        print(s)
