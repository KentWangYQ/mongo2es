# -*- coding: utf-8 -*-

from . import impression_track, act_share_detail


def index():
    impression_track.index()
    act_share_detail.index()


def rt_index(mongo_oplog):
    impression_track.rt_index(mongo_oplog)
    act_share_detail.rt_index(mongo_oplog)
