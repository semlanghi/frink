#!/usr/bin/env bash

export KAFKA_HEAP_OPTS="-Xmx5G -Xms1G"
export BROKER="localhost:9092"
export TOPIC="topic1"

for value in "sum;0;50" "sum;0;100" "sum;0;150" "sum;0;200" "sum;0;250" "sum;0;300"
do
    echo java -cp target/frink-1.0-SNAPSHOT.jar linearroad.LinearRoadRunner --source kafka --mode local --kafka $BROKER --topic $TOPIC --bufferType single_buffer --windowType aggregate --windowParams $value
    java -cp target/frink-1.0-SNAPSHOT.jar linearroad.LinearRoadRunner --source kafka --mode local --kafka $BROKER --topic $TOPIC --bufferType single_buffer --windowType aggregate --windowParams $value
done