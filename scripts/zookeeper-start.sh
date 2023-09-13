#!/usr/bin/env bash

export _JAVA_OPTIONS="-Xmx1g"

if [ -z "$CONFLUENT_HOME" ]
then
      echo "Need to set environment variable CONFLUENT_HOME"
      exit 1;
else
      echo "CONFLUENT_HOME is $CONFLUENT_HOME"
fi

#clean logs:
echo "Cleaning /kafka_log* and zookeeper folders from /tmp"
rm -rf /tmp/zookeeper;

#start zookeeper
echo "Starting zookeeper"
$CONFLUENT_HOME/bin/zookeeper-server-start $CONFLUENT_HOME/etc/kafka/zookeeper.properties &> zookeeper.out &