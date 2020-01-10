import json

from kafka import KafkaProducer
from kafka import KafkaConsumer

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
# producer.send('sample',b'hello world')

# with open('/home/varsha/wikiticker-2015-09-12-sampled.json') as f:
# data = json.load(f)
# print(data)
# producer.flush()


data = []
with open('/home/ramya/PISA_Consolidated_Json/part-00000-6c97ad3b-1a5d-43ab-91e9-239ca8c8db62-c000.json', 'r') as f:
    for line in f:
        # data.append(json.loads(line))
        print(json.loads(line))
        producer.send('Demo_kafka', json.loads(line))
        producer.flush()

# keylist=json.loads(line).keys()
# print(keylist)
