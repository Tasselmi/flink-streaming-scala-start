#!/usr/bin/env bash

cd ~/Documents/learn-kafka/kafka_2.11-2.4.1-bin || exit
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties

bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning

cd ~/Documents/learn-flink/flink-1.10.1-bin || exit
bin/start-cluster.sh