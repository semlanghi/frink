#!/usr/bin/env bash

#export _JAVA_OPTIONS="-Xmx10g"


bootstrap=$1 #server and port kafka is running
topic=$2 # name of topic created and name of topic where producer will produce and consumer will consume
input=$3 # name of dat file?
maxevents=$4 # ?
jobType=$5
if [ -z "$KAFKA_HOME" ]
then
      KAFKA_HOME="/root/platforms/confluent-5.3.1"
else
      echo "KAFKA_HOME is $KAFKA_HOME"
fi

if [ -z "$PROJECT_DIR" ]
then
      PROJECT_DIR="/root/ICEP/ICEP/ICEP"
else
      echo "PROJECT_DIR is $PROJECT_DIR"
fi

# Setup topic
echo "Setting up the topic $topic"
$KAFKA_HOME/bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic "$topic" --if-exists
$KAFKA_HOME/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic "$topic"
sleep 10

# Execute producer
echo "Start producing linear road data to Topic $topic:"
java -Xmx2g -cp $PROJECT_DIR/target/frink-1.0-SNAPSHOT.jar linearroad.datagenerator.LinearRoadKafkaDataProducer ${bootstrap} ${topic} ${input} ${maxevents} #&> loading.out
echo "Producer finished"

echo "Start consuming:"
java -Xmx2g -cp $PROJECT_DIR/target/frink-1.0-SNAPSHOT.jar linearroad.LinearRoadRunner --source kafka --mode local --kafka ${bootstrap} --topic ${topic} --timeMode event  --jobType ${jobType}
echo "Finished consuming"