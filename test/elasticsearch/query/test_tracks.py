# -*- coding: utf-8 -*-

import unittest
import datetime
import json

from module.elasticsearch.query import tracks


class TracksTest(unittest.TestCase):

    def test_pv_count(self):
        create_time = {
            'start': '2018-05-01',
            'end': '2018-09-30'
        }

        host_name = ['show.test.com', 'show.test1.com']

        path = '/share'
        channel_id = '1'
        first_level = 'smallapp'
        second_level = '5a252371667c9f0c62ac8986'
        sender = '55fbeb4744bd9090708b4567'
        tag = ''
        smcid = ['5a6043cc25d11b551e8bbe18', '']
        actid = '5ae445085fb92b6f2b3b2b18'
        result = tracks.impression_track.pv_count(create_time=create_time, host_name=host_name, path=path,
                                                  channel_id=channel_id, first_level=first_level,
                                                  second_level=second_level, sender=sender, tag=tag, smcid=smcid,
                                                  actid=actid)

        print json.dumps(result, encoding='utf-8', ensure_ascii=False)

        # print(result)

    def test_pv_count_wash(self):
        create_time = {
            'start': '2018-05-01',
            'end': '2018-09-30'
        }

        host_name = ['show.test.com', 'show.test1.com']

        path = '/share'
        channel_id = '1'
        first_level = 'smallapp'
        second_level = '5a252371667c9f0c62ac8986'
        sender = '55fbeb4744bd9090708b4567'
        tag = ''
        smcid = '5a6043cc25d11b551e8bbe18'
        actid = '5ae445085fb92b6f2b3b2b18'
        result = tracks.impression_track.pv_count(create_time=create_time, host_name=host_name, path=path,
                                                  channel_id=channel_id, first_level=first_level,
                                                  second_level=second_level, sender=sender, tag=tag, smcid=smcid,
                                                  actid=actid)
        result = tracks.impression_track.pv_count_wash(result)
        print json.dumps(result, encoding='utf-8', ensure_ascii=False)
        # print(result)

    def test_pv_agg(self):
        create_time = {
            'start': '2018-05-01',
            'end': '2018-09-30'
        }
        result = tracks.impression_track.pv_agg(create_time=create_time,
                                                agg_keys=['sender', 'create_time_month', 'actid', 'create_time_day',
                                                          'first_level'])
        print json.dumps(result, encoding='utf-8', ensure_ascii=False)

    def test_pv_agg_wash(self):
        create_time = {
            'start': '2018-05-01',
            'end': '2018-09-30'
        }
        agg_keys = ['sender', 'create_time_month', 'actid', 'create_time_day', 'first_level']
        result = tracks.impression_track.pv_agg(create_time=create_time, agg_keys=agg_keys)
        result = tracks.impression_track.pv_agg_wash(result=result, agg_keys=agg_keys)
        print json.dumps(result, encoding='utf-8', ensure_ascii=False)

    def test_share_count(self):
        create_time = {
            'start': '2017-05-01',
            'end': '2018-09-30'
        }

        host_name = ['show.test.com', 'show.test1.com']

        path = '/share'
        channel_id = '0'
        first_level = 'group'
        second_level = '57eb5737a66da1660c8b4576'
        sender = '57f846b60a4a8ef428a1d11d'
        tag = ''
        smcid = ''
        actid = '58f85d9a7c841097396f23ef'
        result = tracks.act_share_detail.share_count(create_time=create_time, host_name=host_name, path=path,
                                                     channel_id=channel_id, first_level=first_level,
                                                     second_level=second_level, sender=sender, tag=tag, smcid=smcid,
                                                     actid=actid)

        print json.dumps(result, encoding='utf-8', ensure_ascii=False)

        # print(result)

    def test_share_count_wash(self):
        create_time = {
            'start': '2017-05-01',
            'end': '2018-09-30'
        }

        host_name = ['show.test.com', 'show.test1.com']

        path = '/share'
        channel_id = '0'
        first_level = 'group'
        second_level = '57eb5737a66da1660c8b4576'
        sender = '57f846b60a4a8ef428a1d11d'
        tag = ''
        smcid = ''
        actid = '58f85d9a7c841097396f23ef'
        result = tracks.act_share_detail.share_count(create_time=create_time, host_name=host_name, path=path,
                                                     channel_id=channel_id, first_level=first_level,
                                                     second_level=second_level, sender=sender, tag=tag, smcid=smcid,
                                                     actid=actid)
        result = tracks.act_share_detail.share_count_wash(result)
        print json.dumps(result, encoding='utf-8', ensure_ascii=False)
        # print(result)
