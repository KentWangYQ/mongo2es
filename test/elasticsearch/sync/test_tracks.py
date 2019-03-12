# -*- coding: utf-8 -*-

import unittest

from config import settings

settings.SYNC['rts']['mode'] = 'direct'

from module.elasticsearch.sync import tracks
from module.elasticsearch.sync.tracks import impression_track, act_share_detail

from module.elasticsearch.sync.tracks import track_util


class TestLinkActivities(unittest.TestCase):

    def test_impression_track_url_split(self):
        # url = 'http://www.test.com/share?actid=5ba1afe9264f210d4c265588&channelid=0&firstlevel=group&secondlevel=55fbeb4744bd9090708b4567&sender=56654f330ab7e8c903ff7c40&smcid=55fbeb4744bd9090708b4567&tag=salesId_56654f330ab7e8c903ff7c40&preview=1&openid=oHmRUwbcYwx1vIm0OoCZwJFcL9pk&unionid=undefined&STATE=1537323415537.7888",'
        url = 'http://www.test.com//linkshare?actid=5a019a13980de59c0a2b28ec&channelid=7&firstlevel=%E6%B5%8B%E8%AF%95%E5%95%86%E6%88%B7%E6%B5%8B%E8%AF%95%E6%B8%A0%E9%81%93%E7%BA%BF%E4%B8%8B&secondlevel=5a01943ef58a9a421956453a&sender=5a01943ef58a9a421956453a&smcid=5a01943ef58a9a421956453a"'

        result = track_util.url_split(url)
        print(result)

    def test_impression_track_index(self):
        impression_track.index()

    def test_act_share_index(self):
        act_share_detail.index()

    def test_all_index(self):
        tracks.index()
