import json
from kafka import KafkaConsumer

consumer2 = KafkaConsumer('development_oplog',
                          group_id='g2',
                          bootstrap_servers=['192.168.1.203:9092'],
                          value_deserializer=lambda m: json.loads(m.decode('ascii'))
                          )

for message2 in consumer2:
    print("consumer2: %s:%d:%d: key=%s value=%s" % (message2.topic, message2.partition,
                                                    message2.offset, message2.key,
                                                    message2.value))
