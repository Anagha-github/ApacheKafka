#imports
#---------------------------
from sodapy import Socrata
from kafka import KafkaProducer, KafkaConsumer , KafkaClient
import json
import pandas as pd
import pymongo
from pymongo import MongoClient
import matplotlib.pyplot as plt
import dns
#---------------------------

#API data
'''
from sodapy import Socrata
aclient = Socrata("data.cityofnewyork.us", None)
g1 = aclient.get("q5mz-t52e", limit=50000)
g2 = aclient.get("w7fs-fd9i", limit=50000)
g3 = aclient.get("5gj9-2kzx", limit=50000)
g4 = aclient.get("hvrh-b6nb", limit=50000)
y1 = aclient.get("2upf-qytp", limit=50000)
y2 = aclient.get("t29m-gskq", limit=50000)
y3 = aclient.get("biws-g3hs", limit=50000)
y4 = aclient.get("uacg-pexx", limit=50000)
f1 = aclient.get("u6nh-b56h", limit=50000)
f2 = aclient.get("am94-epxh", limit=50000)
f3 = aclient.get("avz8-mqzz", limit=50000)
f4 = aclient.get("yini-w76t", limit=50000)
'''
##################

aclient = Socrata("data.cityofnewyork.us", None)
g19 = aclient.get("q5mz-t52e", limit=5000)
g18 = aclient.get("w7fs-fd9i", limit=5000)
g17 = aclient.get("5gj9-2kzx", limit=5000)
y19 = aclient.get("2upf-qytp", limit=5000)
f17 = aclient.get("avz8-mqzz", limit=5000)

####################
#merge lists for g,y,f

y=y19
f=f17
g19.extend(g18)
g19.extend(g17)
g=g19

'''
g1.extend(g2)
g1.extend(g3)
g1.extend(g4)
g=g1

y1.extend(y2)
y1.extend(y3)
y1.extend(y4)
y=y1

f1.extend(f2)
f1.extend(f3)
f1.extend(f4)
f=f1
'''

# connect to Kafka producer and consumer
kclient = KafkaClient("localhost:9092")
producer = KafkaProducer(bootstrap_servers='localhost:9092')
consumerg = KafkaConsumer('tg',bootstrap_servers=['localhost:9092'],auto_offset_reset="earliest")
consumery = KafkaConsumer('ty',bootstrap_servers=['localhost:9092'],auto_offset_reset="earliest")
consumerf = KafkaConsumer('tf',bootstrap_servers=['localhost:9092'],auto_offset_reset="earliest")


# read API to producer
for row in g:
    producer.send('tg', json.dumps(row).encode('utf-8'))

for row in y:
    producer.send('ty', json.dumps(row).encode('utf-8'))

for row in f:
    producer.send('tf', json.dumps(row).encode('utf-8'))

# store JSON o/p in Mongo
t1=pd.DataFrame()
t2=pd.DataFrame()
t3=pd.DataFrame()

for msg in consumerg:
    a = msg.value.decode("utf-8")
    b = a.replace("'", "\"")
    c = json.loads(b)
    temp = pd.DataFrame.from_records(c, index=[0])
    t1=pd.concat([t1,temp])
    print(a)

for msg in consumery:
    a = msg.value.decode("utf-8")
    b = a.replace("'", "\"")
    c = json.loads(b)
    temp = pd.DataFrame.from_records(c, index=[0])
    t2 = pd.concat([t2, temp])
    print(a)

for msg in consumerf:
    a = msg.value.decode("utf-8")
    b = a.replace("'", "\"")
    c = json.loads(b)
    temp = pd.DataFrame.from_records(c, index=[0])
    t3 = pd.concat([t3, temp])
    print(a)

#data type handling and insertion

#t1

t1['dolocationid']=t1['dolocationid'].astype(int)
t1['extra']=t1['extra'].astype(float)
t1['fare_amount']=t1['fare_amount'].astype(float)
t1['improvement_surcharge']=t1['improvement_surcharge'].astype(float)
t1['lpep_dropoff_datetime']=pd.to_datetime(t1['lpep_dropoff_datetime'])
t1['lpep_pickup_datetime']=pd.to_datetime(t1['lpep_pickup_datetime'])
t1['mta_tax']=t1['mta_tax'].astype(float)
t1['passenger_count']=t1['passenger_count'].astype(int)
t1['payment_type']=t1['payment_type'].astype(int)
t1['pulocationid']=t1['pulocationid'].astype(int)
t1['ratecodeid']=t1['ratecodeid'].astype(int)
t1['tip_amount']=t1['tip_amount'].astype(float)
t1['tolls_amount']=t1['tolls_amount'].astype(float)
t1['total_amount']=t1['total_amount'].astype(float)
t1['trip_distance']=t1['trip_distance'].astype(float)
t1['trip_type']=t1['trip_type'].astype(int)
t1['vendorid']=t1['vendorid'].astype(int)
#t1['store_and_fwd_flag']=t1['store_and_fwd_flag']

#t2

t2['dolocationid']=t2['dolocationid'].astype(int)
t2['extra']=t2['extra'].astype(float)
t2['fare_amount']=t2['fare_amount'].astype(float)
t2['improvement_surcharge']=t2['improvement_surcharge'].astype(float)
t2['tpep_dropoff_datetime']=pd.to_datetime(t2['tpep_dropoff_datetime'])
t2['tpep_pickup_datetime']=pd.to_datetime(t2['tpep_pickup_datetime'])
t2['mta_tax']=t2['mta_tax'].astype(float)
t2['passenger_count']=t2['passenger_count'].astype(int)
t2['payment_type']=t2['payment_type'].astype(int)
t2['pulocationid']=t2['pulocationid'].astype(int)
t2['ratecodeid']=t2['ratecodeid'].astype(int)
t2['tip_amount']=t2['tip_amount'].astype(float)
t2['tolls_amount']=t2['tolls_amount'].astype(float)
t2['total_amount']=t2['total_amount'].astype(float)
t2['trip_distance']=t2['trip_distance'].astype(float)
t2['vendorid']=t2['vendorid'].astype(int)
#t2['store_and_fwd_flag']=t2['store_and_fwd_flag']

#t3

t3['dolocationid']=t3['dolocationid'].astype(float)
t3['dropoff_datetime']=pd.to_datetime(t3['dropoff_datetime'])
t3['pickup_datetime']=pd.to_datetime(t3['pickup_datetime'])
t3['pulocationid']=t3['pulocationid'].astype(float)
#t3['sr_flag']=t2['sr_flag']
#t3['dispatching_base_num']=t3['dispatching_base_num']

#backup
t1.to_excel('E:/SRH/Sem1/Data Engg/Kafka/nt1.xlsx')
t2.to_excel('E:/SRH/Sem1/Data Engg/Kafka/nt2.xlsx')
t3.to_excel('E:/SRH/Sem1/Data Engg/Kafka/nt3.xlsx')

# create database and collection ready
client = MongoClient("mongodb://localhost:27017/")
db = client["nyctaxi"]
ye=db["YellowTaxi"]
fh=db["ForHireTaxi"]
gr=db["GreenTaxi"]
ye.delete_many({})
fh.delete_many({})
gr.delete_many({})
gr.insert_many(t1.to_dict('records'))
ye.insert_many(t2.to_dict('records'))
fh.insert_many(t3.to_dict('records'))


#USER Stories

import pandas as pd
import pymongo
from pymongo import MongoClient
import matplotlib.pyplot as plt

client = MongoClient("mongodb://localhost:27017/")
db = client["nyctaxi"]
ye=db["YellowTaxi"]
fh=db["ForHireTaxi"]
gr=db["GreenTaxi"]


#user story 1 :  I want to know the price rate comparison between green and yellow taxi in 2019

a1 = pd.DataFrame(gr.find({'year': 2019},{'total_amount':1,'trip_distance':1}))
a2 = pd.DataFrame(ye.find({},{'total_amount':1,'trip_distance':1}))

#sum of columns in dataframe a1(green)
m1 = a1['total_amount'].sum()
d1 = a1['trip_distance'].sum()
#sum of columns in dataframe a2(yellow)
m2 = a2['total_amount'].sum()
d2 = a2['trip_distance'].sum()

r1 = m1/d1
r2 = m2/d2
u1 = pd.DataFrame({'Taxi_Type': ['Green Taxi','Yellow Taxi'], 'Price_Rate': [r1,r2]})
u1['Price_Rate']=u1['Price_Rate'].round(2)

fig, ax = plt.subplots()
bar_plot = plt.bar(u1.Taxi_Type,u1.Price_Rate,tick_label=u1.Taxi_Type,color=["green","yellow"])
plt.xlabel('Taxi Type')
plt.ylabel('Price Rate (USD/KM)')
plt.title('Comparison of Green and Yellow Taxi charges in 2019')
def autolabel(rects):
    for idx,rect in enumerate(bar_plot):
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width()/2., height,u1.Price_Rate[idx],ha='center', va='bottom', rotation=0)

autolabel(bar_plot)

#user story 2 : Users with Credit Card Payment in 2019-2018-2017
w = {"payment_type":1}#where
s = {"payment_type":1,'year':1}#select
u = pd.DataFrame(list(gr.find(w, s)))
u=u.drop("_id",axis=1)
u2=pd.DataFrame(u.year.value_counts())
u2['ccusers']=u2['year']
u2=u2.drop("year",axis=1)
u2["year"]=u2.index
u2.index=[0,1,2]

fig, ax = plt.subplots()
bar_plot = plt.bar(u2.year,u2.ccusers,tick_label=u2.year,color=["green","red","blue"])
plt.xlabel('Year')
plt.ylabel('Number of Payments done by Credit Card')
plt.title('Comparison of Green Taxi payment done by credit cards over the years')
def autolabel(rects):
    for idx,rect in enumerate(bar_plot):
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width()/2., height,u2.ccusers[idx],ha='center', va='bottom', rotation=0)

autolabel(bar_plot)


#User Story 3 : To find the total money received by the vendors:
# To understand which vendor earned more on the 1st of January 2019 for the Yellow cabs.
u3= pd.DataFrame(list(ye.aggregate([{ "$group":{ "_id": "$vendorid",
                                                 "totalAmount": { "$sum": "$total_amount" }}
                                      }])))

fig, ax = plt.subplots()
bar_plot = plt.bar([1,2,3],(u3.totalAmount/1000).round(2),tick_label=["VendorID:Four","VendorID:Two","VendorID:One"],color=["green","red","blue"])
plt.xlabel('Vendor IDs')
plt.ylabel('Revenue in 1000 USD')
plt.title('Comparison of Revenue earned by vendors of Yellow Taxi per day')
def autolabel(rects):
    for idx,rect in enumerate(bar_plot):
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width()/2., height,(u3.totalAmount/1000).round(2)[idx],ha='center', va='bottom', rotation=0)

autolabel(bar_plot)


#user story 4 : How many cars were car pooled from the total number of for hire vehicles
u4=(fh.find({"sr_flag":1})).count()
print("The average number of car pools for a day in new york were ",u4,", 22% of the total hired cars")

