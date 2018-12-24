import unittest
import datetime

from module.elasticsearch.query import es_query_util


class EsQueryUtilTest(unittest.TestCase):
    def test_search_date_range_builder(self):
        day = es_query_util.search_date_range_builder()
        print(day)

        day = {
            'start': datetime.datetime.strptime('2018-05-05 01:02:03', '%Y-%m-%d %H:%M:%S'),
            'end': datetime.datetime.strptime('2018-05-28', '%Y-%m-%d')
        }
        day = es_query_util.search_date_range_builder(day)
        print(day)
