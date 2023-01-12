#!/usr/bin/env bash

#export _JAVA_OPTIONS="-Xmx10g"
export PROJECT_DIR="/Users/samuelelanghi/Documents/projects/frink"
export OUT_OF_ORDER_DIR="/Users/samuelelanghi/Documents/projects/out-of-order-datagenerator"


bootstrap=$1 #server and port kafka is running
topic=$2 # name of topic created and name of topic where producer will produce and consumer will consume
input=$3 # name of dat file?
maxevents=$4 # ?
jobType=$5
out_of_order_config=$6

if [ -z "$KAFKA_HOME" ]
then
      KAFKA_HOME="/Users/samuelelanghi/Documents/platforms/confluent-6.2.0"
else
      echo "KAFKA_HOME is $KAFKA_HOME"
fi

if [ -z "$PROJECT_DIR" ]
then
      PROJECT_DIR="/root/ICEP/ICEP/ICEP"
else
      echo "PROJECT_DIR is $PROJECT_DIR"
fi

if [ -z "$OUT_OF_ORDER_DIR" ]
then
      OUT_OF_ORDER_DIR="/root/ICEP/ICEP/ICEP"
      echo "Set OUT_OF_ORDER_DIR env variable to out of order generator"
      exit
else
      echo "OUT_OF_ORDER_DIR is $OUT_OF_ORDER_DIR"
fi


#Preprocessing linear road
echo "Start preprocessing for out of order delays"
mkdir -p data
mkdir -p config

java -Xmx2g -cp $PROJECT_DIR/target/frink-1.0-SNAPSHOT.jar linearroad.dataprocessing.LinearRoadOOOPreProcessing ${input} data/linear-road-preprocessed.txt #&> loading.out
#Locating created file and zipping
tar -czvf data/lr-preprocessed.tgz data/linear-road-preprocessed.txt

echo "Preparing config file for OO"

tmp=$(mktemp)
jq '.outputFilePath = "data/linear-road-"' $out_of_order_config > "$tmp" && mv "$tmp" data/ooo_final_config.json
jq '.rawFilePath = "data/lr-preprocessed.tgz" ' data/ooo_final_config.json > "$tmp" && mv "$tmp" data/ooo_final_config.json


echo "Running out of order generator"
java -Xmx2g -cp $OUT_OF_ORDER_DIR/target/datagenerator-1.0-SNAPSHOT-jar-with-dependencies.jar de.tub.dima.scotty.datagenerator.DataGenerator data/ooo_final_config.json

echo "Finished generating. Running verifier"
out_of_order_output=$(ls -td 'data/linear-roa'* -Art | tail -n 1)
echo "$out_of_order_output"
java -cp $OUT_OF_ORDER_DIR/target/datagenerator-1.0-SNAPSHOT-jar-with-dependencies.jar de.tub.dima.scotty.datagenerator.Verifier -path "$out_of_order_output" -ti 0 --timedomain  ms


echo "Running post processor"
java -Xmx2g -cp $PROJECT_DIR/target/frink-1.0-SNAPSHOT.jar linearroad.dataprocessing.LinearRoadOOOPostProcessing "$out_of_order_output" data/linear-road-out_of_order_post_processed.txt
#Locating created file and zipping


# Setup topic
echo "Setting up the topic $topic"
$KAFKA_HOME/bin/kafka-topics --delete --bootstrap-server localhost:9092 --topic "$topic" --if-exists
$KAFKA_HOME/bin/kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic "$topic"
sleep 3

# Execute producer
echo "Start producing linear road data to Topic $topic:"
java -Xmx2g -cp $PROJECT_DIR/target/frink-1.0-SNAPSHOT.jar linearroad.datagenerator.LinearRoadKafkaDataProducer ${bootstrap} ${topic} data/linear-road-out_of_order_post_processed.txt ${maxevents} #&> loading.out
echo "Producer finished"

echo "Start consuming:"
java -Xmx2g -cp $PROJECT_DIR/target/frink-1.0-SNAPSHOT.jar linearroad.LinearRoadRunner --source kafka --mode local --kafka ${bootstrap} --topic ${topic} --timeMode event  --jobType ${jobType}
echo "Finished consuming"