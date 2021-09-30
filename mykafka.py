# KAFKA


import kafka
from kafka import KafkaProducer, KafkaConsumer , KafkaClient
import csv
import json
import pandas as pd
import pymongo
from pymongo import MongoClient

# create database and collection ready
client = MongoClient("mongodb://localhost:27017/")
db = client["Newtry"]
a = db["col"]
a.delete_many({})
a.insert_many(c.to_dict('records'))

b={'A':[1,2],'B':[10,20]}
c=pd.DataFrame(b)


# connect to Kafka producer and consumer
client = KafkaClient("localhost:9092")
producer = KafkaProducer(bootstrap_servers='localhost:9092')
consumer = KafkaConsumer('t1',bootstrap_servers=['localhost:9092'],auto_offset_reset="earliest")


# read file as dict to get JSON o/p
with open("C:/kafka/t.csv") as file:
    reader = csv.DictReader(file)
    for row in reader:
        producer.send('t1', json.dumps(row).encode('utf-8'))

# store JSON o/p in Mongo
for msg in consumer:
    decoded_msg = msg.value.decode("utf-8")
    b = decoded_msg.replace("'", "\"")
    c = json.loads(b)
    temp = pd.DataFrame.from_records(c, index=[0])
    data.insert_many(temp.to_dict('records'))
    print(decoded_msg)




from sodapy import Socrata
client = Socrata("data.cityofnewyork.us", None)
results = client.get("hvrh-b6nb", limit=5)
results_df = pd.DataFrame.from_records(results)
