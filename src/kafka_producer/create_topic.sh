#!/bin/bash
if [ $# -ne 1 ]; then
  echo "need to specify topic name"
  exit 1
fi
topic=$1
root_dir=/usr/local/kafka/

#/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --partitions 1 --replication-factor 2 --topic kafka_sink

# delete topic if existing
${root_dir}/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic ${topic}


${root_dir}/bin/kafka-topics.sh --create --zookeeper localhost:2181 --partitions 18 --replication-factor 2 --topic ${topic} --config retention.ms=604800000

#python producer.py &
python multiproducer.py >& stdout &

