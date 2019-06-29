# Start services
brew services start zookeeper >/dev/null
brew services start kafka >/dev/null
brew services start influxdb >/dev/null
brew services start grafana >/dev/null

echo Zookeeper, Kafka, InfluxDB, Grafana Started!

