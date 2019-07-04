# Real-Time Twitter Analysis
## Introduction
The purpose of the application is to understand tweet geo-sentiment real-time. I created this app for the Stream Processing class at the Master of Business Analytics & Big Data at IE University. The big picture is using the application to track sentiment of people's tweets and map this into latitude and longitude of the tweets.

Note that not all tweets are geo-enabled so we created a fake Latitude - Longitude coordinate for demonstration purposes.

Our final real time dashboard looks like this:
![Dashboard](https://github.com/stealth-insights/realtime-geosentiment-analysis/blob/master/other/dashboard.jpg)

## Requirements & Setup
The applciation requires the user to install the following packages:
- Zookeeper
- Kafka
- Cassandra
- InfluxDB
- Grafana

I am installing these in MacOS using Homebrew, therefore the start_services.sh and stop_services.sh scripts reference Homebrew. 



## Steps 
We are following this pipeline:
![Pipeline](https://github.com/stealth-insights/realtime-geosentiment-analysis/blob/master/other/pipeline_image.png)


### Start Serices
`sh services/start_services.sh`

### Start Twitter App
`python scripts/twitter_app.py`

#### Check with Kafka Consumer
`kafka-console-consumer --bootstrap-server localhost:9092 --topic tweetstream`

### Start PySpark App
`spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 scripts/spark_transformations.py`

#### Check with Kafka Consumer
`kafka-console-consumer --bootstrap-server localhost:9092 --topic  tweetsprocessed`

### Send data to InfluxDB
`python scripts/writing_to_influxdb.py`

### Start Grafana
`https://localhost:3000/`


### Cleanup 
`sh services/stop_services.sh`



