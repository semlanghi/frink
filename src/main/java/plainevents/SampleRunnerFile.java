package plainevents;

import linearroad.event.CustomStringSchema;
import linearroad.event.FlinkKafkaCustomConsumer;
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
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.PassThroughWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import windowing.ExtendedKeyedStream;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.IllegalFormatFlagsException;
import java.util.Properties;
import java.util.function.BiFunction;
import java.util.function.ToLongFunction;

import static org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09.KEY_POLL_TIMEOUT;

public class SampleRunnerFile {

    private static final long ALLOWED_LATENESS = 5;
    static long windowLength = 1L;

    public static void main(String[] args) throws Exception {

        ParameterTool parameters = ParameterTool.fromArgs(args);
        DataStream<SampleEvent> rawEventStream;
        String mode = parameters.getRequired("mode");
        String bufferType = parameters.getRequired("bufferType");
        String windowType = parameters.getRequired("windowType");
        String windowParams = parameters.get("windowParams");
        String inputFilePath = parameters.get("inputFilePath");
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


       

        rawEventStream = env
                .addSource(new SampleSource(inputFilePath + "sample-" + windowType + "-" + windowParams + ".csv", maxRecords))
                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<SampleEvent>() {
                    @Nullable
                    @Override
                    public Watermark checkAndGetNextWatermark(SampleEvent lastElement, long extractedTimestamp) {
                        return new Watermark(maxTimestampSeen);
                    }

                    private long maxTimestampSeen = 0;

                    @Override
                    public long extractTimestamp(SampleEvent temperatureEvent, long l) {
                        long ts = temperatureEvent.timestamp();
                        // if (temperatureEvent.getKey().equals("W"))
                        maxTimestampSeen = Long.max(maxTimestampSeen, ts);
                        return ts;
                    }
                });

        env.setBufferTimeout(-1);
        env.getConfig().enableObjectReuse();

        long start = System.currentTimeMillis();
        if (bufferType.startsWith("multi_buffer")) {
            ExtendedKeyedStream<SampleEvent, Long> extendedKeyedStream = new ExtendedKeyedStream<>(rawEventStream, SampleEvent::getKey);

            SingleOutputStreamOperator<SampleEvent> sampleEventDataStreamSink;

            String[] params = windowParams.split(";");
            ReduceFunction<SampleEvent> sampleEventReduceFunction = (value1, value2) -> new SampleEvent(value1.getKey(), value1.value() + value2.value(), Math.max(value1.timestamp(), value2.timestamp()));
            if (windowType.endsWith("threshold"))
                sampleEventDataStreamSink = extendedKeyedStream
                        .frameThreshold(Long.parseLong(params[0]), (ToLongFunction<SampleEvent> & Serializable) value -> (long) value.value())
                        .allowedLateness(Time.seconds(ALLOWED_LATENESS))
                        .reduce(sampleEventReduceFunction, new PassThroughWindowFunction<>(), TypeInformation.of(SampleEvent.class));
            else if (windowType.endsWith("delta"))
                sampleEventDataStreamSink = extendedKeyedStream
                        .frameDelta(Long.parseLong(params[0]), (ToLongFunction<SampleEvent> & Serializable) value -> (long) value.value())
                        .allowedLateness(Time.seconds(ALLOWED_LATENESS))
                        .reduce(sampleEventReduceFunction, new PassThroughWindowFunction<>(), TypeInformation.of(SampleEvent.class));
            else if (windowType.endsWith("aggregate")) {



                sampleEventDataStreamSink = extendedKeyedStream
                        .frameAggregate((BiFunction<Long, Long, Long> & Serializable) Long::sum, 0L, Long.parseLong(params[0]), (ToLongFunction<SampleEvent> & Serializable) value -> (long) value.value())
                        .allowedLateness(Time.seconds(ALLOWED_LATENESS))
//                        .reduce((ReduceFunction<SampleEvent>) (value1, value2) -> value1.value() > value2.value() ? value1 : value2, new PassThroughWindowFunction<>(), TypeInformation.of(SampleEvent.class));
                        .reduce(sampleEventReduceFunction, new PassThroughWindowFunction<>(), TypeInformation.of(SampleEvent.class));

            } else throw new IllegalFormatFlagsException("No valid frame specified.");

            DataStream<String> metricsStream = sampleEventDataStreamSink.getSideOutput(metricsSideStream);
            metricsStream.writeAsText("./metrics-" + windowParams + "-" + bufferType + "-" + windowType + " parallelism_" + env.getParallelism() + " - latency", FileSystem.WriteMode.OVERWRITE);

            sampleEventDataStreamSink.writeAsText("./output-" + windowType + "params " + windowParams + "parallelism " + env.getParallelism(), FileSystem.WriteMode.OVERWRITE);
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
            ExtendedKeyedStream<SampleEvent, Long> extendedKeyedStream = new ExtendedKeyedStream<>(rawEventStream, SampleEvent::getKey);

            SingleOutputStreamOperator<SampleEvent> sampleEventDataStreamSink;

            String[] params = windowParams.split(";");
            if (windowType.endsWith("threshold")) {
                sampleEventDataStreamSink = extendedKeyedStream.frameThresholdSingle(Long.parseLong(params[0]), (ToLongFunction<SampleEvent> & Serializable) value -> (long) value.value()).reduce((ReduceFunction<SampleEvent>) (value1, value2) -> value1.value() > value2.value() ? value1 : value2, new PassThroughWindowFunction<>(), TypeInformation.of(SampleEvent.class));
            } else if (windowType.endsWith("delta")) {
                sampleEventDataStreamSink = extendedKeyedStream.frameDeltaSingle(Long.parseLong(params[0]), (ToLongFunction<SampleEvent> & Serializable) value -> (long) value.value()).reduce((ReduceFunction<SampleEvent>) (value1, value2) -> value1.value() > value2.value() ? value1 : value2, new PassThroughWindowFunction<>(), TypeInformation.of(SampleEvent.class));

            } else if (windowType.endsWith("aggregate")) {

                ReduceFunction<SampleEvent> sampleEventReduceFunction = (value1, value2) -> new SampleEvent(value1.getKey(), value1.value() + value2.value(), Math.max(value1.timestamp(), value2.timestamp()));

                sampleEventDataStreamSink = extendedKeyedStream
                        .frameAggregateSingle(
                        (BiFunction<Long, Long, Long> & Serializable) Long::sum,
                        0L, Long.parseLong(params[0]),
                        (ToLongFunction<SampleEvent> & Serializable) value ->
                                (long) value.value())
//                        .reduce((ReduceFunction<SampleEvent>) (value1, value2)
//                                -> value1.value() > value2.value() ? value1 : value2, new PassThroughWindowFunction<>(),
//                        TypeInformation.of(SampleEvent.class));
                        .reduce(sampleEventReduceFunction, new PassThroughWindowFunction<>(), TypeInformation.of(SampleEvent.class));
            } else throw new IllegalFormatFlagsException("No valid frame specified.");

            DataStream<String> latencyStream = sampleEventDataStreamSink.getSideOutput(metricsSideStream);
            latencyStream.writeAsText("./metrics-" + windowParams + "-" + bufferType + "-" + windowType + " parallelism_" + env.getParallelism() + " - latency", FileSystem.WriteMode.OVERWRITE);

            sampleEventDataStreamSink.writeAsText("./output-" + windowParams + "-" + bufferType + "-" + windowType + " parallelism_" + env.getParallelism(), FileSystem.WriteMode.OVERWRITE);
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

