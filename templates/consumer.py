from confluent_kafka import Consumer, KafkaError
import pymongo
from bson import json_util
import json
import bson
mongo_client = pymongo.MongoClient("mongodb+srv://rod:44411599@cluster0-bhtrb.mongodb.net/smart_city?retryWrites=true")
db = mongo_client.smart_city


c = Consumer({
    'bootstrap.servers': '192.168.56.3:9092',
    'group.id': 'teste',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['alerts'])

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue
		
  
    data = json.loads(msg.value())
    print(type(data))
    print(data)
    db.alerts.insert_one(data)

c.close()