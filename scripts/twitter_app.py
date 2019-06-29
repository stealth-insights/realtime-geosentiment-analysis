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


CONSUMER_KEY = 'q4B1COv3l6EJRqvj1yLLQ1fhA'
CONSUMER_SECRET = 'zDtzzyAloIkuD9aYYuPRI03CEaAZFSF6RcI2Vq1JggF9zAfr4g'
ACCESS_TOKEN = '1093833834259533824-BSNwUabcZ1Cx7mBoaOY4Y7RCEjA99y'
ACCESS_SECRET = 'UYdxHvaaK16s3wHIjKe3iDX4LenYLoMg55q7PPgiOqCUF'

# CONSUMER_KEY    = 'w8TL68073oA9fBB8uY4jmXQ1C'
# CONSUMER_SECRET = 'mWkBQcUKu9MFUAGSVOUjAv6EIKHmEwOLoohVYXdTEoq8glcKO1'
# ACCESS_TOKEN    = '870541-Xxzh1Oke7Y4VqWxU6mqjO441TPTJ0xwmE3IjAYjXsLJ'
# ACCESS_SECRET   = 'INTa3rPviJQ0AhAhK4vEceuTo2OkBxAK00f9J7YrWYEd7'

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
