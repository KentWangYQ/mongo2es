# -*- coding: utf-8 -*-

from pymongo import MongoClient

from config import settings

client = MongoClient(settings.MONGO.get('uri'))
models = client.get_database()

from .base import BaseModel

# Brand
brand = models.brands
brand.__class__ = BaseModel

# car_product
car_product = models.carproducts
car_product.__class__ = BaseModel

# merchant
merchant = models.merchants
merchant.__class__ = BaseModel

# car_offer
car_offer = models.caroffers
car_offer.__class__ = BaseModel

# used_car
used_car = models.usedcars
used_car.__class__ = BaseModel

# used_car_offer
used_car_offer = models.usedcaroffers
used_car_offer.__class__ = BaseModel

# car_change_plan
car_change_plan = models.carchangeplans
car_change_plan.__class__ = BaseModel

# order
order = models.orders
order.__class__ = BaseModel

# wx_order
wx_order = models.wxorders
wx_order.__class__ = BaseModel

# pos_order
pos_order = models.posorders
pos_order.__class__ = BaseModel

# user
user = models.users
user.__class__ = BaseModel

# token
token = models.tokens
token.__class__ = BaseModel

# act_entity
act_entity = models.actentities
act_entity.__class__ = BaseModel

# link_action
link_action = models.linkactions
link_action.__class__ = BaseModel

# act_share_detail
act_share_detail = models.actsharedetails
act_share_detail.__class__ = BaseModel

# car_product_map
car_product_map = models.carproductmaps
car_product_map.__class__ = BaseModel

# car_offer_external
car_offer_external = models.carofferexternals
car_offer_external.__class__ = BaseModel

# car_offer_history
car_offer_history = models.carofferhistories
car_offer_history.__class__ = BaseModel

# impression_track
impression_track = models.impressiontracks
impression_track.__class__ = BaseModel

# intents
intent = models.intents
intent.__class__ = BaseModel

# car_product_estimation
car_product_estimation = models.carproductestimations
car_product_estimation.__class__ = BaseModel