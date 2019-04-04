import pymongo

client = pymongo.MongoClient("mongodb://rod:44411599@cluster0-bhtrb.mongodb.net/smart_city?retryWrites=true")
db = client.smart_city
result =  db.alerts.find_one({"teste":123456})

print(result)
