import datetime
import bson
from common.mongo import oplog
from common import event_emitter

mo = oplog.MongoOplog('mongodb://eslocal:PHuance01@172.16.100.150,172.16.100.151,172.16.100.152/?replicaSet=foobar',
                      ts=bson.Timestamp(1524735047, 1))


@event_emitter.on(mo.event_emitter, 'data')
def on_data(data):
    # pass
    print(data)


@event_emitter.on(mo.event_emitter, 'insert')
def on_data(data):
    pass


@event_emitter.on(mo.event_emitter, 'update')
def on_data(data):
    pass


@event_emitter.on(mo.event_emitter, 'delete')
def on_data(data):
    pass


@event_emitter.on(mo.event_emitter, 'cmd')
def on_data(data):
    pass


@event_emitter.on(mo.event_emitter, 'noop')
def on_data(data):
    # print(data)
    pass


mo.tail()
