#!/usr/bin/env bash

export KAFKA_HEAP_OPTS="-Xmx5G -Xms1G"


if [ -z "$CONFLUENT_HOME" ]
then
      echo "Need to set environment variable CONFLUENT_HOME"
      exit 1;
else
      echo "CONFLUENT_HOME is $CONFLUENT_HOME"
fi

#start broker
echo "Starting broker"
$CONFLUENT_HOME/bin/kafka-server-start $CONFLUENT_HOME/etc/kafka/server.properties &> kafka.out &