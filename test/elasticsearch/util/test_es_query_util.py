# -*- coding: utf-8 -*-

import unittest

from module.elasticsearch.query import es_query_util

agg_field_mapping = {
    'create_time_month': {
        "date_histogram": {
            "field": "create_time",
            "interval": "month",
            "format": "yyyy-MM"
        }
    },
    'create_time_day': {
        "date_histogram": {
            "field": "create_time",
            "interval": "day",
            "format": "yyyy-MM-dd"
        }
    },
    'path': {
        "terms": {
            "field": "path.keyword"
        }
    },
    'channel_id': {
        "terms": {
            "field": "params.channelid.keyword"
        }
    },
    'first_level': {
        "terms": {
            "field": "params.firstlevel.keyword"
        }
    },
    'second_level': {
        "terms": {
            "field": "params.secondlevel.keyword"
        }
    },
    'sender': {
        "terms": {
            "field": "params.sender.keyword"
        }
    },
    'tag': {
        "terms": {
            "field": "params.tag.keyword"
        }
    },
    'smcid': {
        "terms": {
            "field": "params.smcid.keyword"
        }
    },
    'actid': {
        "terms": {
            "field": "params.actid.keyword"
        }
    }
}


class TestEsQueryUtil(unittest.TestCase):

    def test_agg_builder(self):
        resutl = es_query_util.agg_builder(agg_field_mapping, ['sender', 'actid'])

        print(resutl)
