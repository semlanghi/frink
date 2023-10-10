export KAFKA_HEAP_OPTS="-Xmx5G -Xms1G"

# DEFAULT VALUES, MODIFIABLE THROUGH --OPTIONS, use --help for more info
bootstrap="localhost:9092"
topic="topic1"
input=""
maxevents=-1
out_of_order_config=""

counter=0
counterMax=$#

echo "Number of script arguments is $counterMax"
while [[ $counter < $counterMax ]]
do
    case "$1" in
        -h|--help)
        echo "User help
        -b|--bootstrap-server : Kafka Server, default 'localhost:9092'
        -t|--topic : Input Topic, default 'topic1'
        -i|--input-file : Required, Dataset File Path
        -m|--max-events : Max Number of events produced, default -1, i.e., whole file
        -o|--ooo-config : used if out of order, path to config file"; exit 0;;
        -b|--bootstrap-server) bootstrap=$2; let "counter+=2"; shift 2;;
        -t|--topic) topic=$2; let "counter+=2"; shift 2;;
        -i|--input-file) input=$2; let "counter+=2"; shift 2;;
        -m|--max-events) maxevents=$2; let "counter+=2"; shift 2;;
        -o|--ooo-config) out_of_order_config=$2; let "counter+=2"; shift 2;;
    esac
done


#Preprocessing linear road
echo "Start preprocessing for out of order delays"
mkdir -p data
mkdir -p config

#java -Xmx2g -cp ../target/frink-1.0-SNAPSHOT.jar linearroad.dataprocessing.LinearRoadOOOPreProcessing ${input} data/linear-road-preprocessed.txt #&> loading.out
#Locating created file and zipping
tar -czvf data/lr-preprocessed.tgz data/linear-road-preprocessed.txt

echo "Preparing config file for OO"

tmp=$(mktemp)
jq '.outputFilePath = "data/linear-road-"' $out_of_order_config > "$tmp" && mv "$tmp" data/ooo_final_config.json
jq '.rawFilePath = "data/lr-preprocessed.tgz" ' data/ooo_final_config.json > "$tmp" && mv "$tmp" data/ooo_final_config.json


echo "Running out of order generator"
java -Xmx2g -cp ../target/frink-1.0-SNAPSHOT.jar ooogenerator.DataGenerator data/ooo_final_config.json

#echo "Finished generating. Running verifier"
out_of_order_output=$(ls -Artd 'data/linear-roa'* | tail -n 1)
echo "$out_of_order_output"
#java -cp $OUT_OF_ORDER_DIR/target/datagenerator-1.0-SNAPSHOT-jar-with-dependencies.jar de.tub.dima.scotty.datagenerator.Verifier -path "$out_of_order_output" -ti 0 --timedomain  ms

#
#echo "Running post processor on $out_of_order_output"
#java -Xmx2g -cp ../target/frink-1.0-SNAPSHOT.jar linearroad.dataprocessing.LinearRoadOOOPostProcessing "$out_of_order_output" "data/linear-road-out_of_order_post_processed.txt"
#sourceFile="data/linear-road-out_of_order_post_processed.txt"