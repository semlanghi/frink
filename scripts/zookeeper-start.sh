#!/usr/bin/env bash

export _JAVA_OPTIONS="-Xmx1g"
export KAFKA_HOME="/Users/samuelelanghi/Documents/platforms/confluent-6.2.0"

if [ -z "$KAFKA_HOME" ]
then
      KAFKA_HOME="/root/platforms/confluent-6.2.0"
else
      echo "KAFKA_HOME is $KAFKA_HOME"
fi

#clean logs:
echo "Cleaning /kafka_log* and zookeeper folders from /tmp"
rm -rf /tmp/zookeeper;

#start zookeeper
echo "Starting zookeeper"
$KAFKA_HOME/bin/zookeeper-server-start $KAFKA_HOME/etc/kafka/zookeeper.properties &> zookeeper.out