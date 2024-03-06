package plainevents;

import linearroad.event.RawEvent;
import org.apache.flink.api.common.functions.ReduceFunction;
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
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.PassThroughWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.OutputTag;
import windowing.ExtendedKeyedStream;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.IllegalFormatFlagsException;
import java.util.function.BiFunction;
import java.util.function.ToLongFunction;

public class InteractiveRunner {

    private static String windowType = "threshold";
    private static String windowParams = "50,300";
    private static Long maxRecords = Long.MAX_VALUE;
    private static final String JOB_TYPE = "frame_multi_" + windowType;
    private static final int ALLOWED_LATENESS = 5;



    public static void main(String[] args) throws Exception {

        ParameterTool parameters = ParameterTool.fromArgs(args);

        final OutputTag<String> latencySideStream = new OutputTag<String>("latency") {
        };
        final OutputTag<String> stateSizeSideStream = new OutputTag<String>("stateSize") {
        };

        // Configuration Creation
        Configuration conf = new Configuration();
        conf.setFloat(TaskManagerOptions.MANAGED_MEMORY_FRACTION, 0.5f);

        // Local Environment creation with parallelism 1 and event time
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(1, conf);
        streamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        streamExecutionEnvironment.getConfig().setAutoWatermarkInterval(100);


        // Creating the source
        DataStream<SampleEvent> rawDataStream = streamExecutionEnvironment
                .addSource(new SampleSource("/Users/samuelelanghi/Documents/projects/frink/scripts/data/sample-" + windowType + "-" + windowParams + ".csv", maxRecords))
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
//
//        streamExecutionEnvironment.setBufferTimeout(-1);
//        streamExecutionEnvironment.getConfig().enableObjectReuse();


        if (JOB_TYPE.startsWith("frame_multi_")) {
            ExtendedKeyedStream<SampleEvent, Long> extendedKeyedStream = new ExtendedKeyedStream<>(rawDataStream, SampleEvent::getKey);

            SingleOutputStreamOperator<SampleEvent> sampleEventDataStreamSink;

            if (JOB_TYPE.endsWith("threshold"))
                sampleEventDataStreamSink = extendedKeyedStream.frameThreshold(50, (ToLongFunction<SampleEvent> & Serializable) value -> (long) value.value())
                        .allowedLateness(Time.seconds(ALLOWED_LATENESS))
                        .reduce((ReduceFunction<SampleEvent>) (value1, value2) -> new SampleEvent(value1.getKey(), value1.value() + value2.value(), Math.max(value1.timestamp(), value2.timestamp())),
                                new PassThroughWindowFunction<>(),
                                TypeInformation.of(SampleEvent.class));
            else if (JOB_TYPE.endsWith("delta"))
                sampleEventDataStreamSink = extendedKeyedStream.frameDelta(50, (ToLongFunction<SampleEvent> & Serializable) value -> (long) value.value())
                        .allowedLateness(Time.seconds(ALLOWED_LATENESS))
                        .reduce((ReduceFunction<SampleEvent>) (value1, value2) -> new SampleEvent(value1.getKey(), value1.value() + value2.value(), Math.max(value1.timestamp(), value2.timestamp())),
                                new PassThroughWindowFunction<>(),
                                TypeInformation.of(SampleEvent.class));
            else if (JOB_TYPE.endsWith("aggregate"))
                sampleEventDataStreamSink = extendedKeyedStream.frameAggregate((BiFunction<Long, Long, Long> & Serializable) Long::sum, 0L, 150, (ToLongFunction<SampleEvent> & Serializable) value -> (long) value.value())
                        .allowedLateness(Time.seconds(ALLOWED_LATENESS))
                        .reduce((ReduceFunction<SampleEvent>) (value1, value2) -> new SampleEvent(value1.getKey(), value1.value() + value2.value(), Math.max(value1.timestamp(), value2.timestamp())),
                                new PassThroughWindowFunction<>(),
                                TypeInformation.of(SampleEvent.class));
            else if (JOB_TYPE.endsWith("tumbling")) {
                ReduceFunction<SampleEvent> sampleEventReduceFunction = (value1, value2) -> new SampleEvent(value1.getKey(), value1.value() + value2.value(), Math.max(value1.timestamp(), value2.timestamp()));
                PassThroughWindowFunction<Object, Window, Object> function = new PassThroughWindowFunction<>();
                sampleEventDataStreamSink = extendedKeyedStream.timeSliding(5000L, 1000L)
                        .allowedLateness(Time.seconds(ALLOWED_LATENESS))
                        .reduce(
                                sampleEventReduceFunction,
                                function,
                                TypeInformation.of(SampleEvent.class));
            } else throw new IllegalFormatFlagsException("No valid frame specified.");

            DataStream<String> latencyStream = sampleEventDataStreamSink.getSideOutput(latencySideStream);
            latencyStream.writeAsText("./output-" + JOB_TYPE + "_alternative" + " parallelism " + streamExecutionEnvironment.getParallelism() + " - latency", FileSystem.WriteMode.OVERWRITE);
            sampleEventDataStreamSink.writeAsText("./output-" + JOB_TYPE + "_alternative" + " parallelism " + streamExecutionEnvironment.getParallelism(), FileSystem.WriteMode.OVERWRITE);
            streamExecutionEnvironment.execute(JOB_TYPE);
        } else if (JOB_TYPE.startsWith("frame_single_")) {
            ExtendedKeyedStream<SampleEvent, Long> extendedKeyedStream = new ExtendedKeyedStream<>(rawDataStream, SampleEvent::getKey);
            SingleOutputStreamOperator<SampleEvent> sampleEventDataStreamSink;

            if (JOB_TYPE.endsWith("threshold"))
                sampleEventDataStreamSink = extendedKeyedStream.frameThresholdSingle(50, (ToLongFunction<SampleEvent> & Serializable) value -> (long) value.value())
                        .allowedLateness(Time.seconds(ALLOWED_LATENESS))
                        .reduce((ReduceFunction<SampleEvent>) (value1, value2) -> new SampleEvent(value1.getKey(), value1.value() + value2.value(), Math.max(value1.timestamp(), value2.timestamp())), new PassThroughWindowFunction<>(),
                                TypeInformation.of(SampleEvent.class));
            else if (JOB_TYPE.endsWith("delta"))
                sampleEventDataStreamSink = extendedKeyedStream.frameDeltaSingle(50, (ToLongFunction<SampleEvent> & Serializable) value -> (long) value.value())
                        .allowedLateness(Time.seconds(ALLOWED_LATENESS))
                        .reduce((ReduceFunction<SampleEvent>) (value1, value2) -> new SampleEvent(value1.getKey(), value1.value() + value2.value(), Math.max(value1.timestamp(), value2.timestamp())),
                                new PassThroughWindowFunction<>(),
                                TypeInformation.of(SampleEvent.class));
            else if (JOB_TYPE.endsWith("aggregate"))
                sampleEventDataStreamSink = extendedKeyedStream.frameAggregateSingle((BiFunction<Long, Long, Long> & Serializable) Long::sum, 0L, 150, (ToLongFunction<SampleEvent> & Serializable) value -> (long) value.value())
                        .allowedLateness(Time.seconds(ALLOWED_LATENESS))
                        .reduce((ReduceFunction<SampleEvent>) (value1, value2) -> new SampleEvent(value1.getKey(), value1.value() + value2.value(), Math.max(value1.timestamp(), value2.timestamp())),
                                new PassThroughWindowFunction<>(),
                                TypeInformation.of(SampleEvent.class));
            else if (JOB_TYPE.endsWith("tumbling")) {
                ReduceFunction<SampleEvent> sampleEventReduceFunction = (value1, value2) -> new SampleEvent(value1.getKey(), value1.value() + value2.value(), Math.max(value1.timestamp(), value2.timestamp()));
                sampleEventDataStreamSink = extendedKeyedStream.timeTumblingSingle(5000L)
                        .allowedLateness(Time.seconds(ALLOWED_LATENESS))
                        .reduce(sampleEventReduceFunction,
                                new PassThroughWindowFunction<>(),
                                TypeInformation.of(SampleEvent.class));
            } else throw new IllegalFormatFlagsException("No valid frame specified.");

            DataStream<String> latencyStream = sampleEventDataStreamSink.getSideOutput(latencySideStream);
            DataStream<String> stateSizeStream = sampleEventDataStreamSink.getSideOutput(stateSizeSideStream);
            latencyStream.writeAsText("./src/main/resources/output-" + JOB_TYPE + "_alternative" + " parallelism " + streamExecutionEnvironment.getParallelism() + " - latency", FileSystem.WriteMode.OVERWRITE);
            stateSizeStream.writeAsText("./src/main/resources/output-" + JOB_TYPE + "_alternative" + " parallelism " + streamExecutionEnvironment.getParallelism() + " - state size", FileSystem.WriteMode.OVERWRITE);

            sampleEventDataStreamSink.writeAsText("./src/main/resources/output-" + JOB_TYPE + " parallelism " + streamExecutionEnvironment.getParallelism(), FileSystem.WriteMode.OVERWRITE);
            streamExecutionEnvironment.execute(JOB_TYPE);
        }

    }

}
