#!/usr/bin/env bash
export _JAVA_OPTIONS="-Xmx1g"

if [ -z "$CONFLUENT_HOME" ]
then
    echo "Need to set environment variable CONFLUENT_HOME"
    exit 1;
else
    echo "CONFLUENT_HOME is $CONFLUENT_HOME"
fi

# stop broker
echo "Stopping broker"
$CONFLUENT_HOME/bin/kafka-server-stop

echo "Cleaning kafka folders from /tmp/"
rm -rf /tmp/kafka-logs*;