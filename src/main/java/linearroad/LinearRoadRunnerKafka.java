package linearroad;

import linearroad.event.RawEvent;
import linearroad.event.SpeedEvent;
import linearroad.event.CustomStringSchema;
import linearroad.event.FlinkKafkaCustomConsumer;
import linearroad.mapper.SpeedMapper;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.api.common.io.ratelimiting.GuavaFlinkConnectorRateLimiter;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.PassThroughWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import windowing.ExtendedKeyedStream;

import java.io.Serializable;
import java.util.IllegalFormatFlagsException;
import java.util.Properties;
import java.util.function.BiFunction;
import java.util.function.ToLongFunction;

import static org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09.KEY_POLL_TIMEOUT;

public class LinearRoadRunnerKafka {

    private static final long ALLOWED_LATENESS = 5;
    static long windowLength = 1L;

    public static void main(String[] args) throws Exception {

        ParameterTool parameters = ParameterTool.fromArgs(args);
        DataStream<SpeedEvent> rawEventStream;
        String mode = parameters.getRequired("mode");
        String kafka = parameters.get("kafka");
        String topic = parameters.get("topic");
        String bufferType = parameters.getRequired("bufferType");
        String windowType = parameters.getRequired("windowType");
        String winParams = parameters.get("windowParams");
        Long maxRecords = parameters.getLong("maxRecords", Long.MAX_VALUE);
        long maxMinutes = parameters.getLong("maxMinutes", 0);
        StreamExecutionEnvironment env;

        if (mode.equalsIgnoreCase("cluster")) env = StreamExecutionEnvironment.getExecutionEnvironment();
        else {
            Configuration conf = new Configuration();
            conf.setFloat(TaskManagerOptions.MANAGED_MEMORY_FRACTION, 0.5f);
            env = StreamExecutionEnvironment.createLocalEnvironment(1, conf);
        }

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);



        final OutputTag<String> metricsSideStream = new OutputTag<String>("latency") {
        };



        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafka);
        properties.setProperty(KEY_POLL_TIMEOUT, "0");


        FlinkKafkaCustomConsumer<String> consumer =
                new FlinkKafkaCustomConsumer<>(
                        topic,
                        new CustomStringSchema(
                                new SimpleStringSchema(),
                                maxMinutes),
                        properties);
        consumer.setStartFromEarliest();

        if (parameters.get("rate") != null) {
            FlinkConnectorRateLimiter rateLimiter = new GuavaFlinkConnectorRateLimiter();
            rateLimiter.setRate(Long.parseLong(parameters.get("rate")));
            consumer.setRateLimiter(rateLimiter);
        }


        consumer.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
            @Override
            public long extractAscendingTimestamp(String element) {
                String[] data = element.replace("[", "").replace("]", "").split(",");
                return Long.parseLong(data[8].trim());
            }
        });

        rawEventStream = env.addSource(consumer).map(new SpeedMapper(bufferType, windowType, winParams, env.getParallelism()));


        env.setBufferTimeout(-1);
        env.getConfig().enableObjectReuse();

        long start = System.currentTimeMillis();
        if (bufferType.startsWith("multi_buffer")) {
            ExtendedKeyedStream<SpeedEvent, String> extendedKeyedStream = new ExtendedKeyedStream<>(rawEventStream, RawEvent::getKey);

            SingleOutputStreamOperator<SpeedEvent> speedEventDataStreamSink;

            if (windowType.endsWith("threshold"))
                speedEventDataStreamSink = extendedKeyedStream
                        .frameThreshold(50, (ToLongFunction<SpeedEvent> & Serializable) value -> (long) value.getValue())
                        .allowedLateness(Time.seconds(ALLOWED_LATENESS))
                        .reduce((ReduceFunction<SpeedEvent>) (value1, value2) -> value1.getValue() > value2.getValue() ? value1 : value2, new PassThroughWindowFunction<>(), TypeInformation.of(SpeedEvent.class));
            else if (windowType.endsWith("delta"))
                speedEventDataStreamSink = extendedKeyedStream
                        .frameDelta(50, (ToLongFunction<SpeedEvent> & Serializable) value -> (long) value.getValue())
                        .allowedLateness(Time.seconds(ALLOWED_LATENESS))
                        .reduce((ReduceFunction<SpeedEvent>) (value1, value2) -> value1.getValue() > value2.getValue() ? value1 : value2, new PassThroughWindowFunction<>(), TypeInformation.of(SpeedEvent.class));
            else if (windowType.endsWith("aggregate")) {
                String[] params = winParams.split(";");
                ReduceFunction<SpeedEvent> speedEventReduceFunction = (value1, value2) -> new SpeedEvent(value1.getKey(), Math.max(value1.getTimestamp(), value2.getTimestamp()), value1.getValue() + value2.getValue());

                speedEventDataStreamSink = extendedKeyedStream
                        .frameAggregate((BiFunction<Long, Long, Long> & Serializable) Long::sum, Long.parseLong(params[1]), Long.parseLong(params[2]), (ToLongFunction<SpeedEvent> & Serializable) value -> (long) value.getValue())
                        .allowedLateness(Time.seconds(ALLOWED_LATENESS))
//                        .reduce((ReduceFunction<SpeedEvent>) (value1, value2) -> value1.getValue() > value2.getValue() ? value1 : value2, new PassThroughWindowFunction<>(), TypeInformation.of(SpeedEvent.class));
                        .reduce(speedEventReduceFunction, new PassThroughWindowFunction<>(), TypeInformation.of(SpeedEvent.class));

            } else throw new IllegalFormatFlagsException("No valid frame specified.");

            DataStream<String> metricsStream = speedEventDataStreamSink.getSideOutput(metricsSideStream);
            metricsStream.writeAsText("./metrics-" + winParams + "-" + bufferType + "-" + windowType + " parallelism_" + env.getParallelism() + " - latency", FileSystem.WriteMode.OVERWRITE);

            speedEventDataStreamSink.writeAsText("./output-" + windowType + "params " + winParams + "parallelism " + env.getParallelism(), FileSystem.WriteMode.OVERWRITE);
            env.execute(windowType);
            //TODO time-based window
//            String winLen = parameters.get("windowSize");
//
//            if (winLen != null) {
//                try {
//                    windowLength = Long.parseLong(winLen);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
        } else if (bufferType.startsWith("single_buffer")) {
            ExtendedKeyedStream<SpeedEvent, String> extendedKeyedStream = new ExtendedKeyedStream<>(rawEventStream, RawEvent::getKey);

            SingleOutputStreamOperator<SpeedEvent> speedEventDataStreamSink;

            if (windowType.endsWith("threshold")) {
                speedEventDataStreamSink = extendedKeyedStream.frameThresholdSingle(50, (ToLongFunction<SpeedEvent> & Serializable) value -> (long) value.getValue()).reduce((ReduceFunction<SpeedEvent>) (value1, value2) -> value1.getValue() > value2.getValue() ? value1 : value2, new PassThroughWindowFunction<>(), TypeInformation.of(SpeedEvent.class));
            } else if (windowType.endsWith("delta")) {
                speedEventDataStreamSink = extendedKeyedStream.frameDeltaSingle(50, (ToLongFunction<SpeedEvent> & Serializable) value -> (long) value.getValue()).reduce((ReduceFunction<SpeedEvent>) (value1, value2) -> value1.getValue() > value2.getValue() ? value1 : value2, new PassThroughWindowFunction<>(), TypeInformation.of(SpeedEvent.class));

            } else if (windowType.endsWith("aggregate")) {
                String[] params = winParams.split(";");
                ReduceFunction<SpeedEvent> speedEventReduceFunction = (value1, value2) -> new SpeedEvent(value1.getKey(), Math.max(value1.getTimestamp(), value2.getTimestamp()), value1.getValue() + value2.getValue());

                speedEventDataStreamSink = extendedKeyedStream
                        .frameAggregateSingle(
                        (BiFunction<Long, Long, Long> & Serializable) Long::sum,
                        Long.parseLong(params[1]), Long.parseLong(params[2]),
                        (ToLongFunction<SpeedEvent> & Serializable) value ->
                                (long) value.getValue())
//                        .reduce((ReduceFunction<SpeedEvent>) (value1, value2)
//                                -> value1.getValue() > value2.getValue() ? value1 : value2, new PassThroughWindowFunction<>(),
//                        TypeInformation.of(SpeedEvent.class));
                        .reduce(speedEventReduceFunction, new PassThroughWindowFunction<>(), TypeInformation.of(SpeedEvent.class));
            } else throw new IllegalFormatFlagsException("No valid frame specified.");

            DataStream<String> latencyStream = speedEventDataStreamSink.getSideOutput(metricsSideStream);
            latencyStream.writeAsText("./metrics-" + winParams + "-" + bufferType + "-" + windowType + " parallelism_" + env.getParallelism() + " - latency", FileSystem.WriteMode.OVERWRITE);

            speedEventDataStreamSink.writeAsText("./output-" + winParams + "-" + bufferType + "-" + windowType + " parallelism_" + env.getParallelism(), FileSystem.WriteMode.OVERWRITE);
            env.execute(windowType);
        }

        System.out.println("Time taken: " + (System.currentTimeMillis() - start) + " ms");
    }

//    private static BiFunction<Long, Long, Long> getFunction(String param) {
//        switch (param) {
//            case "max":
//                return (Serializable) Long::max;
//            case "min":
//                return Long::min;
//            case "sum":
//            default:
//                return Long::sum;
//        }
//    }

}

