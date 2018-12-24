import unittest

from common.elasticsearch.elasticsearch_client.elasticsearch_client import es_client


class ElasticsearchTest(unittest.TestCase):
    def test_search(self):
        result = es_client.search(index='carchangeplans-test', doc_type='carchangeplan')

        for doc in result['hits']['hits']:
            print(doc)

    def test_query_ids(self):
        ids = es_client.query_ids(index='p-carchangeplans', doc_type='usedcar', query={})
