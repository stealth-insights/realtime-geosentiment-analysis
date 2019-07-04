# Location Streamer
import time
import socket
import sys
import requests
import requests_oauthlib
import json
import tweepy
import time
from random import uniform
from kafka import KafkaProducer


latitude = 54.5260   # geographical centre of search
longitude = 15.2551   # geographical centre of search
max_range = 10000


CONSUMER_KEY = '******'
CONSUMER_SECRET = '******'
ACCESS_TOKEN = '******'
ACCESS_SECRET = '******'

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: x.encode())

def get_data():
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
    api = tweepy.API(auth, wait_on_rate_limit=True,
                     wait_on_rate_limit_notify=True)
    return api


def send_data(api):
    for tweet in tweepy.Cursor(api.search,
                               q="",
                               geocode=f"{latitude},{longitude},{max_range}km").items():
        time.sleep(1)
        if tweet.geo is not None:
       
            text = tweet.text
            text = text.replace('\n', ' ')
            geo = tweet.geo

            # Get LatLong from Tweet
            lat = geo['coordinates'][0]
            long = geo['coordinates'][1]

            data = text+" ~ "+str(lat)+" ~ "+str(long)
            print(data)
            producer.send('tweetstream', (data))
            print("=========================")

        else:
          
            text = tweet.text
            text = text.replace('\n', ' ')

            # Generate fake Lat Long
            x, y = round(uniform(-11, 17), 4), round(uniform(69, 36), 4)

            lat = y
            long = x

            data = text+" ~ "+str(lat)+" ~ "+str(long)
            print(data)
            producer.send('tweetstream', (data))
            print("=========================")


if __name__ == '__main__':
    print("Connected...")
    api = get_data()
    send_data(api)
