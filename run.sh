#!/usr/bin/env bash



export KAFKA_HEAP_OPTS="-Xmx5G -Xms1G"

# DEFAULT VALUES, MODIFIABLE THROUGH --OPTIONS, use --help for more info
BROKER="localhost:9092"
TOPIC="topic1"
mode="local"
bufferType="multi_buffer"
windowType="aggregate"


counter=0
while [[ counter -lt $# ]]
do
    case "$1" in
        -h|--help)
        echo "User help
        -b|--bootstrap-server : Kafka Server, default 'localhost:9092'
        -t|--topic : Input Topic, defailt 'topic1'
        -l|--local : no argument, flink is execute in local mode, which is default
        -c|--cluster : no argument, flink is execute in cluster mode
        -w|--bufferType : 'multi_buffer' or 'single_buffer'
        -f|--windowType) : frame selected, 'aggregate' 'delta' or 'threshold'"; exit 0;;
        -b|--bootstrap-server) BROKER=$2; let "counter+=2"; shift 2;;
        -t|--topic) TOPIC=$2; let "counter+=2"; shift 2;;
        -l|--local) mode="local"; shift 1;;
        -c|--cluster) mode="cluster"; shift 1;;
        -w|--bufferType) bufferType=$2; let "counter+=2"; shift 2;;
        -f|--windowType) windowType=$2; let "counter+=2"; shift 2;;
    esac
done


for value in "sum;0;50" "sum;0;100" "sum;0;150" "sum;0;200" "sum;0;250" "sum;0;300"
do
    echo "Start consuming from kafka topic $TOPIC with parameters $value:"
    echo java -cp target/frink-1.0-SNAPSHOT.jar linearroad.LinearRoadRunnerKafka --mode ${mode} --kafka $BROKER --topic $TOPIC --bufferType $bufferType --windowType $windowType --windowParams $value
    java -cp target/frink-1.0-SNAPSHOT.jar linearroad.LinearRoadRunnerKafka --mode ${mode} --kafka $BROKER --topic $TOPIC --bufferType $bufferType --windowType $windowType --windowParams $value &> run.out &
    wait
    echo "Finished consuming."
done

#java -Xmx2g -cp ../target/frink-1.0-SNAPSHOT.jar linearroad.LinearRoadRunnerKafka --kafka ${bootstrap} --topic ${topic} --jobType ${jobType}

