# FRINK: FRames on Apache FlINK

FRINK is an extension of the Flink Windowing library. In particular, it tries to import data-driven windows (i.e., Frames) on top of Flink 
windowing mechanism. The implementation is based on the [this paper](https://dl.acm.org/doi/abs/10.1145/2933267.2933304), by Grossniklaus et al.
We cover all 4 cases of data-driven windows described in the paper:

- Threshold Windows, which holds data above a certain threshold
- Boundary Windows, which divide the data according to given value boundaries
- Delta Windows, which holds data with a values difference under a given threshold
- Aggregate Windows, which aggregate data based on the satisfaction of a rolling aggregate condition


## Guidelines - Synthetic Events Dataset

1. Create the files: start `./scripts/create-sample-files.sh` with params, some relevant are (check `./scripts/create-sample-files.sh --help` for params explanations)
   - directory location where to place the dataset files
   - window parameters
   - maxevents to produce
   - out of order configuration file path, if needed.
2. Once `create-sample-files.sh` has finished, run the experiments with `./run-sample.sh` (check again `./run-sample.sh --help` for parameters explanations)


## Guidelines - Kafka LinerRoad Pipeline

1. Set Environment Variable CONFLUENT_HOME to your confluent directory
2. Start zookeeper 
3. Start kafka node
4. Produce the data: start `./scripts/load_improved.sh` with params, some relevant are (check `./scripts/load_improved --help` for params explanations)
   - kafka bootstrap server 
   - topic name where to read data from
   - path where linear road data set is, could be the full dataset
   - maxevents to produce
   - out of order configuration file path, if needed.
5. Once `load_improved.sh` has finished, run the experiments with `./run.sh` (check again `./run.sh --help` for params explanations)



Expanation of pipeline:

1. Use input file and do preprocessing; i.e make the file format ready to feed to out of order generator;
2. Update paths in input out of order config and use that as the final config
3. Run our of order generator on input file
4. Post process the file with out of order in dat format, ready to be fed to Flink Kafka Consumer and Kafka producer
5. Create-Drop topic
6. Publish max Events to kafka topic
7. Start LinearRoadRunner for jobtype.





