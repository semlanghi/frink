1. Start zookeeper
2. Start kafka node
3. Start load.sh with params: (Example: ./load.sh localhost:9092 linear-road     /home/ekeshi/Documents/diploma/first-flink-job/linear_road_out_of_order.dat 1000 frame_multi_aggregate ./out_of_order_config.json
   )
   - kafka bootstrap server
   - topic name where to read and write data from
   - path where linear road data set is, could be the full dataset or filtered one.
   - maxevents to publish. It will make pipeline run faster as only maxevents events will be  published in kafka queue and read from flink kafka consumer. However
It is still not very fast, because Out of order generator will still work on the whole file, depending on the configuration.
   - frame_multi_aggregate represents the job type that frink runner should execute.
   - last arg is out of order config. Pay attention that out_of_order_config given as input is changed (outputFilePath and Raw File path don't ) matter
as they are changed inside the pipeline to automate the flow.



Expanation of pipeline:

1. Use input file and do preprocessing; i.e make the file format ready to feed to out of order generator;
2. Update paths in input out of order config and use that as the final config
3. Run our of order generator on input file
4. Post process the file with out of order in dat format, ready to be fed to Flink Kafka Consumer and Kafka producer
5. Create-Drop topic
6. Publish max Events to kafka topic
7. Start LinearRoadRunner for jobtype.

Important env variables.
$PROJECT DIR- FRINK base dir
$OUT_OF_ORDER_DIT='out of order generator dir".

It is important to have packaged both frink and out of order projects before running the pipeline.