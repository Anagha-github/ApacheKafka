from kafka import KafkaProducer, KafkaConsumer , KafkaClient
import csv
import json
import pandas as pd
import pymongo
from pymongo import MongoClient

producer = KafkaProducer(bootstrap_servers='localhost:9092')

with open("C:/kafka/t.csv") as file:
    reader = csv.DictReader(file)
    for row in reader:
        producer.send('taxi', json.dumps(row).encode('utf-8'))