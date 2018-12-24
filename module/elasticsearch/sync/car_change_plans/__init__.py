# -*- coding: utf-8 -*-

from . import used_car, used_car_offer, car_change_plan, car_offer, order, wx_order, pos_order


def index():
    used_car.index()
    used_car_offer.index()
    car_change_plan.index()
    car_offer.index()
    order.index()
    wx_order.index()
    pos_order.index()


def rt_index(mongo_oplog):
    used_car.rt_index(mongo_oplog)
    used_car_offer.rt_index(mongo_oplog)
    car_change_plan.rt_index(mongo_oplog)
    car_offer.rt_index(mongo_oplog)
    order.rt_index(mongo_oplog)
    wx_order.rt_index(mongo_oplog)
    pos_order.rt_index(mongo_oplog)
