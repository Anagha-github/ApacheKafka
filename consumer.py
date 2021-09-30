from kafka import KafkaProducer, KafkaConsumer , KafkaClient
import csv
import json
import pandas as pd
import pymongo
from pymongo import MongoClient


client = MongoClient("mongodb://localhost:27017/")
db = client["Taxi"]
data = db["Data"]
data.delete_many({})

consumer = KafkaConsumer('taxi',bootstrap_servers=['localhost:9092'])

for msg in consumer:
    decoded_msg = msg.value.decode("utf-8")
    print(decoded_msg)