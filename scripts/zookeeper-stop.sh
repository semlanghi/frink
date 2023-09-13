#!/usr/bin/env bash
export _JAVA_OPTIONS="-Xmx1g"

if [ -z "$CONFLUENT_HOME" ]
then
      echo "Need to set environment variable CONFLUENT_HOME"
      exit 1;
else
      echo "CONFLUENT_HOME is $CONFLUENT_HOME"
fi

# stop zookeeper
echo "Stopping zookeeper"
$CONFLUENT_HOME/bin/zookeeper-server-stop $CONFLUENT_HOME/config/zookeeper.properties

#clean logs:
echo "Cleaning zookeeper folders from /tmp"
rm -rf /tmp/zookeeper;