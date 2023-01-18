package linearroad;

import event.RawEvent;
import event.SpeedEvent;
import linearroad.event.CustomStringSchema;
import linearroad.event.FlinkKafkaCustomConsumer;
import linearroad.mapper.SpeedMapper;
import linearroad.source.LinearRoadSource;
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
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.PassThroughWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import windowing.ExtendedKeyedStream;

import java.io.Serializable;
import java.util.IllegalFormatFlagsException;
import java.util.Properties;
import java.util.function.BiFunction;
import java.util.function.ToLongFunction;

import static org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09.KEY_POLL_TIMEOUT;

public class LinearRoadRunner {

    static long windowLength = 1L;

    public static void main(String[] args) throws Exception {

        ParameterTool parameters = ParameterTool.fromArgs(args);
        DataStream<SpeedEvent> rawEventStream;
        String source = parameters.getRequired("source");
        String mode = parameters.getRequired("mode");
        String kafka;
        String fileName;
        String topic;
        String bufferType;
        String windowType;
        String numRecordsToEmit;
        StreamExecutionEnvironment env;

        if (mode.equalsIgnoreCase("cluster")) env = StreamExecutionEnvironment.getExecutionEnvironment();
        else {
            Configuration conf = new Configuration();
            conf.setFloat(TaskManagerOptions.MANAGED_MEMORY_FRACTION, 0.5f);
            env = StreamExecutionEnvironment.createLocalEnvironment(1, conf);
        }

        int iNumRecordsToEmit = Integer.MAX_VALUE;

        bufferType = parameters.getRequired("bufferType");
        windowType = parameters.getRequired("windowType");

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String winParams = parameters.get("windowParams");

        if (source.toLowerCase().equals("kafka")) {
            kafka = parameters.get("kafka");
            topic = parameters.get("topic");
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", kafka);
            properties.setProperty(KEY_POLL_TIMEOUT, "0");

            FlinkKafkaCustomConsumer<String> consumer =
                    new FlinkKafkaCustomConsumer<>(
                            topic,
                            new CustomStringSchema(
                                    new SimpleStringSchema(),
                                    parameters.getLong("maxMinutes", 0)),
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


        } else {
            fileName = parameters.get("fileName");
            rawEventStream = env.addSource(new LinearRoadSource(fileName, iNumRecordsToEmit));


            if (env.getStreamTimeCharacteristic() == TimeCharacteristic.EventTime) {
                rawEventStream = rawEventStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<SpeedEvent>() {
                    private long maxTimestampSeen = 0;

                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(maxTimestampSeen);
                    }

                    @Override
                    public long extractTimestamp(SpeedEvent temperatureEvent, long l) {
                        long ts = temperatureEvent.getTimestamp();
                        maxTimestampSeen = Long.max(maxTimestampSeen, l);
                        return ts;
                    }
                });
            }
        }

        env.setBufferTimeout(-1);
        env.getConfig().enableObjectReuse();

        long start = System.currentTimeMillis();
        if (bufferType.startsWith("multi_buffer")) {
            ExtendedKeyedStream<SpeedEvent, String> extendedKeyedStream = new ExtendedKeyedStream<>(rawEventStream, RawEvent::getKey);

            SingleOutputStreamOperator<SpeedEvent> speedEventDataStreamSink;

            if (windowType.endsWith("threshold"))
                speedEventDataStreamSink = extendedKeyedStream.frameThreshold(50, (ToLongFunction<SpeedEvent> & Serializable) value -> (long) value.getValue()).reduce((ReduceFunction<SpeedEvent>) (value1, value2) -> value1.getValue() > value2.getValue() ? value1 : value2, new PassThroughWindowFunction<>(), TypeInformation.of(SpeedEvent.class));
            else if (windowType.endsWith("delta"))
                speedEventDataStreamSink = extendedKeyedStream.frameDelta(50, (ToLongFunction<SpeedEvent> & Serializable) value -> (long) value.getValue()).reduce((ReduceFunction<SpeedEvent>) (value1, value2) -> value1.getValue() > value2.getValue() ? value1 : value2, new PassThroughWindowFunction<>(), TypeInformation.of(SpeedEvent.class));
            else if (windowType.endsWith("aggregate")) {
                String[] params = winParams.split(";");
                speedEventDataStreamSink = extendedKeyedStream.frameAggregate((BiFunction<Long, Long, Long> & Serializable) Long::sum, Long.parseLong(params[1]), Long.parseLong(params[2]), (ToLongFunction<SpeedEvent> & Serializable) value -> (long) value.getValue()).reduce((ReduceFunction<SpeedEvent>) (value1, value2) -> value1.getValue() > value2.getValue() ? value1 : value2, new PassThroughWindowFunction<>(), TypeInformation.of(SpeedEvent.class));
            } else throw new IllegalFormatFlagsException("No valid frame specified.");

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
                speedEventDataStreamSink = extendedKeyedStream.frameAggregateSingle(
                        (BiFunction<Long, Long, Long> & Serializable) Long::sum,
                        Long.parseLong(params[1]), Long.parseLong(params[2]),
                        (ToLongFunction<SpeedEvent> & Serializable) value ->
                                (long) value.getValue()).reduce((ReduceFunction<SpeedEvent>) (value1, value2)
                                -> value1.getValue() > value2.getValue() ? value1 : value2, new PassThroughWindowFunction<>(),
                        TypeInformation.of(SpeedEvent.class));
            } else throw new IllegalFormatFlagsException("No valid frame specified.");


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

