import datetime, json
from kafka import KafkaProducer
from kafka.errors import KafkaError

# m = json.dumps({'now': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}).encode('utf-8')
# r = json.loads('{"now": "%Y-%m-%d %H:%M:%S"}')

producer = KafkaProducer(bootstrap_servers=['192.168.1.203:9092'],
                         value_serializer=lambda m: json.dumps(m).encode('utf-8'))
future = producer.send('Kent_topic', {'now': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')})

try:
    record_metadata = future.get(timeout=10)
except KafkaError as ex:
    # Decide what to do if produce request failed...
    print(ex)
    pass

print(record_metadata.topic)
print(record_metadata.partition)
print(record_metadata.offset)
