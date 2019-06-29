import sys
from kafka import KafkaConsumer
import json
import time
import requests
from influxdb import InfluxDBClient
import rfc3339
import datetime

# from cassandra.cluster import Cluster
# from cassandra.cluster import Cluster

consumer = KafkaConsumer('tweetsprocessed', bootstrap_servers="localhost:9092")

# cluster = Cluster(['localhost'])
# session = cluster.connect('twitter')  # keyspace

client = InfluxDBClient(host='localhost', port=8086)
client.drop_database('tweets-influx')
client.create_database('tweets-influx')
print(client.get_list_database())

c = 0
try:
    for message in consumer:
        values = json.loads(message.value.decode())

        print(values['Tweet'])
        print("--------------------------------------------------")

# TO CASSANDRA
#         c = c + 1
#         session.execute(
#             """
# INSERT INTO tweets (key, tweet, lat, long, city, state, country, positive, negative, neutral, overallsentiment)
# VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
# """,
#             (str(c),
#              values['Tweet'],
#              values['Lat'],
#              values['Long'],
#              values['City'],
#              values['State'],
#              values['Country'],
#              values['Positive'],
#              values['Negative'],
#              values['Neutral'],
#              values['OverallSentiment']))

# TO INFLUXDB

        json_body = [{
                "measurement": "tweets",
                "time": rfc3339.rfc3339(datetime.datetime.now()),
                "fields": {
                    "tweet": values['Tweet'],
                     "lat": float(values['Lat']),
                     "long": float(values['Long']),
                     "negative": float(values['Negative']),
                     "positive": float(values['Positive']),
                     "neutral": float(values['Neutral']),
                     "overallsentiment": float(values['OverallSentiment']),
                     "city": values['City'],
                     "state": values['State'],
                     "country": values['Country']
                     }}]
        client.write_points(points=json_body, database='tweets-influx')


except KeyboardInterrupt:
    sys.exit()