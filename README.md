# Real-Time Twitter Analysis

## Introduction


## Requirements & Setup



## Steps 

### Start Serices
sh /services/start_services.sh

### Start Twitter App
python /scripts/twitter_app.py

#### Check with Kafka Consumer
kafka-console-consumer --bootstrap-server localhost:9092 --topic tweetstream


### Start PySpark App
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 /scripts/spark_transformations.py


#### Check with Kafka Consumer
kafka-console-consumer --bootstrap-server localhost:9092 --topic  tweetsprocessed


### Send data to InfluxDB
python /scripts/writing_to_influxdb.py


### Start Grafana

https://localhost:3000/



### Cleanup 
sh /services/stop_services.sh



