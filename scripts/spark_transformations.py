from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pyspark.sql.functions import udf
from time import sleep
import re
from geopy.geocoders import Nominatim
from pyspark.streaming.kafka import KafkaUtils
import pyspark.sql.functions as f
from pyspark.sql.functions import lit
import datetime


spark = SparkSession.builder\
                    .appName('Tweet Sentiment Analysis')\
                    .getOrCreate()

print(" ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
print(" ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
print(" ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
print(" ++++++++++++++++++++++++++++++++++++START SPARK ++++++++++++++++++++++++++++++++++++")
print(" ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
print(" ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
print(" ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

location = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "tweetstream") \
  .option("startingOffsets", "latest") \
  .option("failOnDataLoss", "false") \
  .load()

location = location.selectExpr("CAST(value AS STRING)")

print("Are we streaming? " + str(location.isStreaming))
print(" ")

print("Data Schema:")
location.printSchema()
print(" ")

# Separate data

split_col = split(location["value"], "~")

location = location\
    .withColumn('Tweet', split_col.getItem(0))\
    .withColumn('Lat', split_col.getItem(1))\
    .withColumn('Long', split_col.getItem(2))


# Get Location
geolocator = Nominatim(user_agent="Tweet Location")


@udf()
def find_country(lat, long):
    try:
        latlong = lat + ", " + long
        location = geolocator.reverse(latlong)
        return location.raw["address"]['country']
    except:
        return 'No country'


@udf()
def find_state(lat, long):
    try:
        latlong = lat + ", " + long
        location = geolocator.reverse(latlong)
        return location.raw["address"]['state']
    except:
        return 'No state'


@udf()
def find_city(lat, long):
    try:
        latlong = lat + ", " + long
        location = geolocator.reverse(latlong)
        return location.raw["address"]['city']
    except:
        return 'No city'


location = location\
    .withColumn('City', find_city(location.Lat, location.Long))\
    .withColumn('State', find_state(location.Lat, location.Long))\
    .withColumn('Country', find_country(location.Lat, location.Long))


# Get Sentiment
analyser = SentimentIntensityAnalyzer()
sentiment_analyzer_udf = udf(lambda text: analyser.polarity_scores(text))
compound_sentiment_udf = udf(lambda score: score.get('compound'))
positive_sentiment_udf = udf(lambda score: score.get('pos'))
negative_sentiment_udf = udf(lambda score: score.get('neg'))
neutral_sentiment_udf = udf(lambda score: score.get('neu'))

location = location\
    .withColumn('Sentiment', sentiment_analyzer_udf(location.Tweet))\

location = location\
    .withColumn('Positive', positive_sentiment_udf(location.Sentiment))\
    .withColumn('Negative', negative_sentiment_udf(location.Sentiment))\
    .withColumn('Neutral', neutral_sentiment_udf(location.Sentiment))\
    .withColumn('OverallSentiment', compound_sentiment_udf(location.Sentiment))\
    .drop('Sentiment')

# # Hashtag 
# get_hashtag_udf = udf(lambda text: re.findall(r"\B(\#[a-zA-Z]+\b)", text))

# location = location\
#     .withColumn('Hashtags', get_hashtag_udf(location.Tweet))

location = location.filter(f.col('State') != "No state")

# Set Time Key
location = location\
    .withColumn('key', lit(datetime.datetime.now()))\
    .drop('value')


location.printSchema()
# Start stream

locationStream = location\
        .selectExpr("CAST(key AS STRING)", "to_json(struct(*)) AS value")\
        .writeStream\
        .format('kafka')\
        .option('kafka.bootstrap.servers', 'localhost:9092')\
        .option('topic', 'tweetsprocessed')\
        .option('checkpointLocation', '/Users/sebastianmontero/Desktop/Location Data/checkpoints')\
        .start()

locationStream.awaitTermination()

# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 pyspark_analysis_kafka.py




# Analyzing streams

# all_tweets = "SELECT * FROM Places"

# sentiment_by_state = '''
# SELECT avg(OverallSentiment), count(OverallSentiment), State
# FROM Places
# GROUP BY State
# ORDER BY avg(OverallSentiment) DESC
# '''

# all_tweets2 = '''
# SELECT *, WINDOW(time, "1 second") FROM Places
# '''

# sentiment_by_city = '''
# SELECT avg(OverallSentiment), count(OverallSentiment), City
# FROM Places
# GROUP BY City
# ORDER BY avg(OverallSentiment) DESC
# '''

# while True:
#     spark.sql(all_tweets).show()
#     sleep(3)

