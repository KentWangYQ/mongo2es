import thread
from common.kafka.consumer import Consumer
from common import event_emitter

consumer = Consumer('development_DataExport')

thread.start_new_thread(consumer.tail, ())


@event_emitter.on(consumer.event_emitter, 'message')
def message(msg):
    print(msg)
