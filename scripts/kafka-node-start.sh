#!/usr/bin/env bash

export KAFKA_HEAP_OPTS="-Xmx5G -Xms1G"
#export KAFKA_HOME="/home/ekeshi/kafka_2.13-3.2.0"
export KAFKA_HOME="/Users/samuelelanghi/Documents/platforms/confluent-6.2.0"

broker_count=$1

if [ -z "$KAFKA_HOME" ]
then
      KAFKA_HOME="/root/platforms/confluent-6.2.0"
else
      echo "KAFKA_HOME is $KAFKA_HOME"
fi

if [ -z "$PROJECT_DIR" ]
then
      PROJECT_DIR="/root/ICEP/ICEP/ICEP"
else
      echo "PROJECT_DIR is $PROJECT_DIR"
fi




#start broker
echo "Starting broker"
$KAFKA_HOME/bin/kafka-server-start $KAFKA_HOME/etc/kafka/server.properties &