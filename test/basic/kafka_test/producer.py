from datetime import datetime

from kafka.errors import KafkaError

from common.kafka.producer import Producer

producer = Producer()

future = producer.send('kent_topic', {'now': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})

try:
    record_metadata = future.get(timeout=10)
except KafkaError as ex:
    # Decide what to do if produce request failed...
    print(ex)
    pass

print(record_metadata.topic)
print(record_metadata.partition)
print(record_metadata.offset)
