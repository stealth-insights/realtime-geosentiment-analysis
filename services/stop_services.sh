# Stop services
brew services stop zookeeper >/dev/null
brew services stop kafka >/dev/null
brew services stop influxdb >/dev/null
brew services stop grafana >/dev/null

echo Zookeeper, Kafka, InfluxDB, Grafana Stopped