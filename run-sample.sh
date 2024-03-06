#!/usr/bin/env bash


# DEFAULT VALUES, MODIFIABLE THROUGH --OPTIONS, use --help for more info
mode="local"
bufferType="all"
windowType="all"
inputFilePath="./scripts/sampledata/"
allowedLateness=10


counter=0
while [[ counter -lt $# ]]
do
    case "$1" in
        -h|--help)
        echo "User help
        -l|--local : no argument, flink is execute in local mode, which is default
        -c|--cluster : no argument, flink is execute in cluster mode
        -p|--file-path : input file path, default './scripts/sampledata/'
        -w|--bufferType : 'all' (default), 'multi_buffer' or 'single_buffer'
        -f|--windowType) : frame selected, 'aggregate' 'delta' or 'threshold' or default 'all'
        -a|--allowedLateness : define the allowed lateness (in seconds) after which records are evicted, default 10"; exit 0;;
        -l|--local) mode="local"; let "counter+=1"; shift 1;;
        -c|--cluster) mode="cluster"; let "counter+=1"; shift 1;;
        -p|--file-path) inputFilePath=$2; let "counter+=2"; shift 2;;
        -w|--bufferType) bufferType=$2; let "counter+=2"; shift 2;;
        -f|--windowType) windowType=$2; let "counter+=2"; shift 2;;
        -a|--allowedLateness) allowedLateness=$2; let "counter+=2"; shift 2;;
    esac
done

if [ "$windowType" == "all" ]; then
    windowTypeList=("threshold" "aggregate" "delta");
else windowTypeList=("$windowType");
fi

if [ "$bufferType" == "all" ]; then
    bufferTypeList=("multi_buffer" "single_buffer");
else bufferTypeList=("$bufferType");
fi

# Window Parameters: n1;n2;n3:
# n1 = threshold for data-driven windows
# n2 = size of the windows in terms of tuples,
# n3 = ONLY USED FOR TIME-BASED windows, window slide

for value in "1000;50" "1000;100"; do
  for tmpWindowType in "${windowTypeList[@]}"; do
    for tmpBufferType in "${bufferTypeList[@]}"; do
        echo "Start consuming from kafka topic $TOPIC with parameters $value with Long.MAX allowedLateness (no eviction):"
        echo java -Xmx2g -cp target/frink-1.0-SNAPSHOT.jar plainevents.SampleRunnerFile --mode ${mode} --inputFilePath "$inputFilePath" --bufferType $tmpBufferType --windowType $tmpWindowType --windowParams $value
        java -Xmx2g -cp target/frink-1.0-SNAPSHOT.jar plainevents.SampleRunnerFile --mode ${mode} --inputFilePath "$inputFilePath" --bufferType $tmpBufferType --windowType $tmpWindowType --windowParams $value &> run${tmpWindowType}-${tmpBufferType}-${value}.out &
        wait
        echo "Finished consuming."
        echo "Start consuming from kafka topic $TOPIC with parameters $value with allowedLateness $allowedLateness:"
        echo java -Xmx2g -cp target/frink-1.0-SNAPSHOT.jar plainevents.SampleRunnerFile --mode ${mode} --allowedLateness "${allowedLateness}" --inputFilePath "$inputFilePath" --bufferType $tmpBufferType --windowType $tmpWindowType --windowParams $value
        java -Xmx2g -cp target/frink-1.0-SNAPSHOT.jar plainevents.SampleRunnerFile --mode ${mode} --allowedLateness "${allowedLateness}" --inputFilePath "$inputFilePath" --bufferType $tmpBufferType --windowType $tmpWindowType --windowParams $value &> run${tmpWindowType}-${tmpBufferType}-${value}.out &
        wait
        echo "Finished consuming."
    done
  done
done

#java -Xmx2g -cp ../target/frink-1.0-SNAPSHOT.jar linearroad.LinearRoadRunnerKafka --kafka ${bootstrap} --topic ${topic} --jobType ${jobType}

