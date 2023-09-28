#!/usr/bin/env bash


# DEFAULT VALUES, MODIFIABLE THROUGH --OPTIONS, use --help for more info
mode="local"
bufferType="multi_buffer"
windowType="all"
inputFilePath="./scripts/sampledata/"


counter=0
while [[ counter -lt $# ]]
do
    case "$1" in
        -h|--help)
        echo "User help
        -l|--local : no argument, flink is execute in local mode, which is default
        -c|--cluster : no argument, flink is execute in cluster mode
        -p|--file-path : input file path, default './scripts/sampledata/'
        -w|--bufferType : 'multi_buffer' (default) or 'single_buffer'
        -f|--windowType) : frame selected, 'aggregate' 'delta' or 'threshold' or default 'all'"; exit 0;;
        -l|--local) mode="local"; let "counter+=1"; shift 1;;
        -c|--cluster) mode="cluster"; let "counter+=1"; shift 1;;
        -p|--file-path) inputFilePath=$2; let "counter+=2"; shift 2;;
        -w|--bufferType) bufferType=$2; let "counter+=2"; shift 2;;
        -f|--windowType) windowType=$2; let "counter+=2"; shift 2;;
    esac
done

if [ "$windowType" == "all" ]; then
    windowTypeList=("threshold" "aggregate" "delta");
else windowTypeList=("$windowType");
fi

for value in "1000;50" "1000;100" "1000;150" "1000;200" "1000;250" "1000;300"; do
  for tmpWindowType in "${windowTypeList[@]}"; do
        echo "Start consuming from kafka topic $TOPIC with parameters $value:"
        echo java -Xmx2g -cp target/frink-1.0-SNAPSHOT.jar plainevents.SampleRunnerFile --mode ${mode} --inputFilePath "$inputFilePath" --bufferType $bufferType --windowType $tmpWindowType --windowParams $value
        java -Xmx2g -cp target/frink-1.0-SNAPSHOT.jar plainevents.SampleRunnerFile --mode ${mode} --inputFilePath "$inputFilePath" --bufferType $bufferType --windowType $tmpWindowType --windowParams $value &> run${tmpWindowType}-${bufferType}-${value}.out &
        wait
        echo "Finished consuming."
  done
done

#java -Xmx2g -cp ../target/frink-1.0-SNAPSHOT.jar linearroad.LinearRoadRunnerKafka --kafka ${bootstrap} --topic ${topic} --jobType ${jobType}

