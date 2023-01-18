import event.RawEvent;
import event.SpeedEvent;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.PassThroughWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import windowing.ExtendedKeyedStream;


import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.IllegalFormatFlagsException;
import java.util.function.BiFunction;
import java.util.function.ToLongFunction;

public class InteractiveRunner {

    private static final String JOB_TYPE = "frame_multi_aggregate";
    private static final int ALLOWED_LATENESS = 5;

    public static void main(String[] args) throws Exception {

        final OutputTag<String> outputTag = new OutputTag<String>("latency"){};

        // Configuration Creation
        Configuration conf = new Configuration();
        conf.setFloat( TaskManagerOptions.MANAGED_MEMORY_FRACTION, 0.5f);

        // Local Environment creation with parallelism 1 and event time
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(1, conf);
        streamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        streamExecutionEnvironment.getConfig().setAutoWatermarkInterval(100);


        // Creating the source
        DataStream<SpeedEvent> rawDataStream = streamExecutionEnvironment.addSource(new FixedSource()).assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<SpeedEvent>() {
            @Nullable
            @Override
            public Watermark checkAndGetNextWatermark(SpeedEvent lastElement, long extractedTimestamp) {
                return new Watermark(maxTimestampSeen);
            }

            private long maxTimestampSeen = 0;

            @Override
            public long extractTimestamp(SpeedEvent temperatureEvent, long l) {
                long ts = temperatureEvent.getTimestamp();
                // if (temperatureEvent.getKey().equals("W"))
                maxTimestampSeen = Long.max(maxTimestampSeen, ts);
                return ts;
            }
        });
//
//        streamExecutionEnvironment.setBufferTimeout(-1);
//        streamExecutionEnvironment.getConfig().enableObjectReuse();


        if (JOB_TYPE.startsWith("frame_multi_")){
            ExtendedKeyedStream<SpeedEvent,String> extendedKeyedStream = new ExtendedKeyedStream<>(rawDataStream, RawEvent::getKey);

            SingleOutputStreamOperator<SpeedEvent> speedEventDataStreamSink;

            if(JOB_TYPE.endsWith("threshold"))
                speedEventDataStreamSink = extendedKeyedStream.frameThreshold(50, (ToLongFunction<SpeedEvent> & Serializable) value -> (long) value.getValue())
                        .allowedLateness(Time.seconds(ALLOWED_LATENESS))
                        .reduce((ReduceFunction<SpeedEvent>) (value1, value2) -> new SpeedEvent(value1.getKey(), Math.max(value1.getTimestamp(), value2.getTimestamp()), value1.getValue() + value2.getValue()),
                        new PassThroughWindowFunction<>(),
                        TypeInformation.of(SpeedEvent.class));
            else if(JOB_TYPE.endsWith("delta"))
                speedEventDataStreamSink = extendedKeyedStream.frameDelta(50, (ToLongFunction<SpeedEvent> & Serializable) value -> (long) value.getValue())
                        .allowedLateness(Time.seconds(ALLOWED_LATENESS))
                        .reduce((ReduceFunction<SpeedEvent>) (value1, value2) -> new SpeedEvent(value1.getKey(), Math.max(value1.getTimestamp(), value2.getTimestamp()), value1.getValue() + value2.getValue()),
                        new PassThroughWindowFunction<>(),
                        TypeInformation.of(SpeedEvent.class));
            else if(JOB_TYPE.endsWith("aggregate"))
                speedEventDataStreamSink = extendedKeyedStream.frameAggregate((BiFunction<Long, Long, Long> & Serializable) Long::sum, 0L, 150, (ToLongFunction<SpeedEvent> & Serializable) value -> (long) value.getValue())
                        .allowedLateness(Time.seconds(ALLOWED_LATENESS))
                        .reduce((ReduceFunction<SpeedEvent>) (value1, value2) -> new SpeedEvent(value1.getKey(), Math.max(value1.getTimestamp(), value2.getTimestamp()), value1.getValue() + value2.getValue()),
                        new PassThroughWindowFunction<>(),
                        TypeInformation.of(SpeedEvent.class));
            else throw new IllegalFormatFlagsException("No valid frame specified.");

            DataStream<String> latencyStream = speedEventDataStreamSink.getSideOutput(outputTag);
            latencyStream.writeAsText("./src/main/resources/output-" + JOB_TYPE + "_alternative" + " parallelism " + streamExecutionEnvironment.getParallelism() + " - latency" , FileSystem.WriteMode.OVERWRITE);
            speedEventDataStreamSink.writeAsText("./src/main/resources/output-" + JOB_TYPE + "_alternative" + " parallelism " + streamExecutionEnvironment.getParallelism() , FileSystem.WriteMode.OVERWRITE);
            streamExecutionEnvironment.execute(JOB_TYPE);
        }else if(JOB_TYPE.startsWith("frame_single_")) {
            ExtendedKeyedStream<SpeedEvent, String> extendedKeyedStream = new ExtendedKeyedStream<>(rawDataStream, RawEvent::getKey);

            SingleOutputStreamOperator<SpeedEvent> speedEventDataStreamSink;

            if (JOB_TYPE.endsWith("threshold"))
                speedEventDataStreamSink = extendedKeyedStream.frameThresholdSingle(50, (ToLongFunction<SpeedEvent> & Serializable) value -> (long) value.getValue())
                        .allowedLateness(Time.seconds(ALLOWED_LATENESS))
                        .reduce((ReduceFunction<SpeedEvent>) (value1, value2) -> new SpeedEvent(value1.getKey(), Math.max(value1.getTimestamp(), value2.getTimestamp()), value1.getValue() + value2.getValue()),                        new PassThroughWindowFunction<>(),
                        TypeInformation.of(SpeedEvent.class));
            else if (JOB_TYPE.endsWith("delta"))
                speedEventDataStreamSink = extendedKeyedStream.frameDeltaSingle(50, (ToLongFunction<SpeedEvent> & Serializable) value -> (long) value.getValue())
                        .allowedLateness(Time.seconds(ALLOWED_LATENESS))
                        .reduce((ReduceFunction<SpeedEvent>) (value1, value2) -> new SpeedEvent(value1.getKey(), Math.max(value1.getTimestamp(), value2.getTimestamp()), value1.getValue() + value2.getValue()),
                        new PassThroughWindowFunction<>(),
                        TypeInformation.of(SpeedEvent.class));
            else if (JOB_TYPE.endsWith("aggregate"))
                speedEventDataStreamSink = extendedKeyedStream.frameAggregateSingle((BiFunction<Long, Long, Long> & Serializable) Long::sum, 0L, 150, (ToLongFunction<SpeedEvent> & Serializable) value -> (long) value.getValue())
                        .allowedLateness(Time.seconds(ALLOWED_LATENESS))
                        .reduce((ReduceFunction<SpeedEvent>) (value1, value2) -> new SpeedEvent(value1.getKey(), Math.max(value1.getTimestamp(), value2.getTimestamp()), value1.getValue() + value2.getValue()),
                        new PassThroughWindowFunction<>(),
                        TypeInformation.of(SpeedEvent.class));
            else throw new IllegalFormatFlagsException("No valid frame specified.");

            DataStream<String> latencyStream = speedEventDataStreamSink.getSideOutput(outputTag);
            latencyStream.writeAsText("./src/main/resources/output-" + JOB_TYPE + "_alternative" + " parallelism " + streamExecutionEnvironment.getParallelism() + " - latency" , FileSystem.WriteMode.OVERWRITE);

            speedEventDataStreamSink.writeAsText("./src/main/resources/output-" + JOB_TYPE + " parallelism " + streamExecutionEnvironment.getParallelism(), FileSystem.WriteMode.OVERWRITE);
            streamExecutionEnvironment.execute(JOB_TYPE);
        }

    }

    public static class FixedSource implements SourceFunction<SpeedEvent> {
        private volatile boolean running = true;
        @Override
        public void run(SourceContext<SpeedEvent> ctx) {
            if (running)
            {
                ctx.collectWithTimestamp(new SpeedEvent("1", 1000, 55),1000);
                ctx.collectWithTimestamp(new SpeedEvent("1", 4000, 56),4000);
                ctx.collectWithTimestamp(new SpeedEvent("1", 7000, 57),7000);
                ctx.collectWithTimestamp(new SpeedEvent("1", 8000, 130),8000);
                ctx.collectWithTimestamp(new SpeedEvent("1", 10000, 30),10000);
                ctx.collectWithTimestamp(new SpeedEvent("1", 11000, 120),11000);
                ctx.collectWithTimestamp(new SpeedEvent("1", 12000, 50),12000);
                ctx.collectWithTimestamp(new SpeedEvent("1", 13000, 40),13000);
                ctx.collectWithTimestamp(new SpeedEvent("1", 14000, 60),14000);
                ctx.collectWithTimestamp(new SpeedEvent("1", 15000, 130),15000);


                ctx.collectWithTimestamp(new SpeedEvent("1", 5000, 130),5000);
                ctx.collectWithTimestamp(new SpeedEvent("1", 9000, 50),9000);
                ctx.collectWithTimestamp(new SpeedEvent("1", 4500, 30),4500);

                ctx.collectWithTimestamp(new SpeedEvent("1", 16000, 130),15000);

                ctx.collectWithTimestamp(new SpeedEvent("1", 24500, 30),24500);
                ctx.collectWithTimestamp(new SpeedEvent("1", 25500, 30),25500);
                ctx.collectWithTimestamp(new SpeedEvent("1", 26500, 30),26500);
                ctx.collectWithTimestamp(new SpeedEvent("1", 27500, 30),27500);
                ctx.collectWithTimestamp(new SpeedEvent("1", 28500, 30),28500);


            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
