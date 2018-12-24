import json
import thread
from kafka import KafkaConsumer

consumer1 = KafkaConsumer('qproduction_oplog',
                          group_id='g1',
                          bootstrap_servers=['172.16.100.60:9092',
                                             '172.16.100.61:9092'],
                          value_deserializer=lambda m: json.loads(m.decode('utf-8'))
                          )

consumer1.poll(max_records=1)

p = consumer1.assignment()

try:
    consumer1.seek_to_end()
except:
    pass


def do():
    for message1 in consumer1:
        print("consumer1: %s:%d:%d: key=%s value=%s" % (message1.topic, message1.partition,
                                                        message1.offset, message1.key,
                                                        message1.value))


do()
# thread.start_new_thread(do, ())

print('end')
