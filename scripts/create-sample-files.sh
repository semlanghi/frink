#!/usr/bin/env bash


# DEFAULT VALUES, MODIFIABLE THROUGH --OPTIONS, use --help for more info
input="./sampledata/"
maxevents=0
out_of_order_config=""
windowType="all"

counter=0
counterMax=$#

echo "Number of script arguments is $counterMax"
while [[ $counter < $counterMax ]]
do
    case "$1" in
        -h|--help)
        echo "User help
        -i|--file-path : Datasets File Paths, default is './sampledata/'
        -m|--max-events : Max Number of events created, REQUIRED
        -o|--ooo-config : used if out of order, path to config file;
        -f|--windowType) : frame selected, 'aggregate' 'delta' or 'threshold' or default 'all'"; exit 0;;
        -i|--file-path) input=$2; let "counter+=2"; shift 2;;
        -m|--max-events) maxevents=$2; let "counter+=2"; shift 2;;
        -o|--ooo-config) out_of_order_config=$2; let "counter+=2"; shift 2;;
        -f|--windowType) windowType=$2; let "counter+=2"; shift 2;;
    esac
done

if [ ${maxevents} == 0 ]; then
    echo "--max-events option not set. Exiting..."
    exit 0;
fi

mkdir sampledata

if [ -z "$CONFLUENT_HOME" ]
then
      echo "Need to set environment variable CONFLUENT_HOME"
      exit 1;
else
      echo "CONFLUENT_HOME is $CONFLUENT_HOME"
fi


if [ ${input} == "" ]
then
      echo "Need to set a file directory."
      exit 1;
else
      echo "File Directory is $input"
fi



if [ $windowType != "all" ]; then
  windowTypeList=("$windowType");
else windowTypeList=("threshold" "aggregate" "delta");
fi



for value in "1000;50" "1000;100" "1000;150" "1000;200" "1000;250" "1000;300"; do
  for tmpWindowType in "${windowTypeList[@]}"; do
    echo "Starting Creating file for configuration (threshold;size) = $value"
    echo java -Xmx2g -cp ../target/frink-1.0-SNAPSHOT.jar plainevents.datagenerator.SampleEventFileBuilder --path ${input} --windowParams "$value" --windowType ${tmpWindowType} --max-events ${maxevents} #&> loading.out
    java -Xmx2g -cp ../target/frink-1.0-SNAPSHOT.jar plainevents.datagenerator.SampleEventFileBuilder --path ${input} --windowParams "$value" --windowType ${tmpWindowType} --max-events ${maxevents} #&> loading.out
    echo "File creation finished."
    wait


#    if [[ $out_of_order_config != "" ]]; then
#      #Preprocessing linear road
#      echo "Start preprocessing for out of order delays"
#      mkdir -p data
#      mkdir -p config
#
#      java -Xmx2g -cp ../target/frink-1.0-SNAPSHOT.jar linearroad.dataprocessing.LinearRoadOOOPreProcessing ${input} data/linear-road-preprocessed.txt #&> loading.out
#      #Locating created file and zipping
#      tar -czvf data/lr-preprocessed.tgz data/linear-road-preprocessed.txt
#
#      echo "Preparing config file for OO"
#
#      tmp=$(mktemp)
#      jq '.outputFilePath = "data/linear-road-"' $out_of_order_config > "$tmp" && mv "$tmp" data/ooo_final_config.json
#      jq '.rawFilePath = "data/lr-preprocessed.tgz" ' data/ooo_final_config.json > "$tmp" && mv "$tmp" data/ooo_final_config.json
#
#
#      echo "Running out of order generator"
#      java -Xmx2g -cp ../target/frink-1.0-SNAPSHOT.jar linearroad.ooo.DataGenerator data/ooo_final_config.json
#
#      #echo "Finished generating. Running verifier"
#      out_of_order_output=$(ls -Artd 'data/linear-roa'* | tail -n 1)
#      echo "$out_of_order_output"
#      #java -cp $OUT_OF_ORDER_DIR/target/datagenerator-1.0-SNAPSHOT-jar-with-dependencies.jar de.tub.dima.scotty.datagenerator.Verifier -path "$out_of_order_output" -ti 0 --timedomain  ms
#
#
#      echo "Running post processor on $out_of_order_output"
#      java -Xmx2g -cp ../target/frink-1.0-SNAPSHOT.jar linearroad.dataprocessing.LinearRoadOOOPostProcessing "$out_of_order_output" "data/linear-road-out_of_order_post_processed.txt"
#      sourceFile="data/linear-road-out_of_order_post_processed.txt"
#    fi
  done
done




