from cassandra.cluster import Cluster
import time
import requests
import json
from influxdb import InfluxDBClient
import rfc3339
import datetime
from random import randint


cluster = Cluster(['localhost'])
session = cluster.connect('twitter')
query = "SELECT * from tweets"
rows = session.execute(query)

client = InfluxDBClient(host='localhost', port=8086)

client.drop_database('tweets-influx-testing')
client.create_database('tweets-influx-testing')
print(client.get_list_database())

for row in rows:
    time.sleep(randint(1, 3))
    json_body = [{
    "measurement": "tweets",
    "time": rfc3339.rfc3339(datetime.datetime.now()),
    "fields":{
                "tweet": row.tweet,
                 "lat": float(row.lat),
                 "long": float(row.long),
                 "negative": float(row.negative),
                 "positive": float(row.positive),
                 "neutral": float(row.neutral),
                 "overallsentiment": float(row.overallsentiment),
                 "city": row.city,
                 "state": row.state,
                 "country": row.country
                 }}]
    print(json_body)
    client.write_points(points=json_body, database='tweets-influx-testing')
