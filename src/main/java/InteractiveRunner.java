import event.RawEvent;
import event.SpeedEvent;
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
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.PassThroughWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import windowing.ExtendedKeyedStream;


import java.io.Serializable;
import java.util.IllegalFormatFlagsException;
import java.util.function.BiFunction;
import java.util.function.ToLongFunction;

public class InteractiveRunner {

    private static final String JOB_TYPE = "frame_multi_threshold";
    private static final String PRE_PATH = "/Users/samuelelanghi/Documents/projects/frink/src/main/resources";

    public static void main(String[] args) throws Exception {

        // Configuration Creation
        Configuration conf = new Configuration();
        conf.setFloat( TaskManagerOptions.MANAGED_MEMORY_FRACTION, 0.5f);

        // Local Environment creation with parallelism 1 and event time
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(1, conf);
        streamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);



        // Creating the source
        DataStream<SpeedEvent> rawDataStream = streamExecutionEnvironment.addSource(new FixedSource()).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<SpeedEvent>() {
            private long maxTimestampSeen = 0;

            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(maxTimestampSeen);
            }

            @Override
            public long extractTimestamp(SpeedEvent temperatureEvent, long l) {
                long ts = temperatureEvent.getTimestamp();
                // if (temperatureEvent.getKey().equals("W"))
                maxTimestampSeen = Long.max(maxTimestampSeen, l);
                return ts;
            }
        });

        streamExecutionEnvironment.setBufferTimeout(-1);
        streamExecutionEnvironment.getConfig().enableObjectReuse();


        if (JOB_TYPE.startsWith("frame_multi_")){
            ExtendedKeyedStream<SpeedEvent,String> extendedKeyedStream = new ExtendedKeyedStream<>(rawDataStream, RawEvent::getKey);

            SingleOutputStreamOperator<SpeedEvent> speedEventDataStreamSink;

            if(JOB_TYPE.endsWith("threshold"))
                speedEventDataStreamSink = extendedKeyedStream.frameThreshold(50, (ToLongFunction<SpeedEvent> & Serializable) value -> (long) value.getValue()).reduce(
                        (ReduceFunction<SpeedEvent>) (value1, value2) -> new SpeedEvent(value1.getKey(), Math.max(value1.getTimestamp(), value2.getTimestamp()), value1.getValue() + value2.getValue()),
                        new PassThroughWindowFunction<>(),
                        TypeInformation.of(SpeedEvent.class));
            else if(JOB_TYPE.endsWith("delta"))
                speedEventDataStreamSink = extendedKeyedStream.frameDelta(50, (ToLongFunction<SpeedEvent> & Serializable) value -> (long) value.getValue()).reduce(
                        (ReduceFunction<SpeedEvent>) (value1, value2) -> value1.getValue() > value2.getValue() ? value1 : value2,
                        new PassThroughWindowFunction<>(),
                        TypeInformation.of(SpeedEvent.class));
            else if(JOB_TYPE.endsWith("aggregate"))
                speedEventDataStreamSink = extendedKeyedStream.frameAggregate((BiFunction<Long, Long, Long> & Serializable) Long::sum, 0L, 50, (ToLongFunction<SpeedEvent> & Serializable) value -> (long) value.getValue()).reduce(
                        (ReduceFunction<SpeedEvent>) (value1, value2) -> value1.getValue() > value2.getValue() ? value1 : value2,
                        new PassThroughWindowFunction<>(),
                        TypeInformation.of(SpeedEvent.class));
            else throw new IllegalFormatFlagsException("No valid frame specified.");

            speedEventDataStreamSink.writeAsText("./src/main/resources/output-" + JOB_TYPE + " parallelism " + streamExecutionEnvironment.getParallelism(), FileSystem.WriteMode.OVERWRITE);
            streamExecutionEnvironment.execute(JOB_TYPE);
        }else if(JOB_TYPE.startsWith("frame_single_")) {
            ExtendedKeyedStream<SpeedEvent, String> extendedKeyedStream = new ExtendedKeyedStream<>(rawDataStream, RawEvent::getKey);

            SingleOutputStreamOperator<SpeedEvent> speedEventDataStreamSink;

            if (JOB_TYPE.endsWith("threshold"))
                speedEventDataStreamSink = extendedKeyedStream.frameThresholdSingle(50, (ToLongFunction<SpeedEvent> & Serializable) value -> (long) value.getValue()).reduce(
                        (ReduceFunction<SpeedEvent>) (value1, value2) -> value1.getValue() > value2.getValue() ? value1 : value2,
                        new PassThroughWindowFunction<>(),
                        TypeInformation.of(SpeedEvent.class));
            else if (JOB_TYPE.endsWith("delta"))
                speedEventDataStreamSink = extendedKeyedStream.frameDeltaSingle(50, (ToLongFunction<SpeedEvent> & Serializable) value -> (long) value.getValue()).reduce(
                        (ReduceFunction<SpeedEvent>) (value1, value2) -> value1.getValue() > value2.getValue() ? value1 : value2,
                        new PassThroughWindowFunction<>(),
                        TypeInformation.of(SpeedEvent.class));
            else if (JOB_TYPE.endsWith("aggregate"))
                speedEventDataStreamSink = extendedKeyedStream.frameAggregateSingle((BiFunction<Long, Long, Long> & Serializable) Long::sum, 0L, 50, (ToLongFunction<SpeedEvent> & Serializable) value -> (long) value.getValue()).reduce(
                        (ReduceFunction<SpeedEvent>) (value1, value2) -> value1.getValue() > value2.getValue() ? value1 : value2,
                        new PassThroughWindowFunction<>(),
                        TypeInformation.of(SpeedEvent.class));
            else throw new IllegalFormatFlagsException("No valid frame specified.");

            speedEventDataStreamSink.writeAsText("./src/main/resources/output-" + JOB_TYPE + " parallelism " + streamExecutionEnvironment.getParallelism(), FileSystem.WriteMode.OVERWRITE);
            streamExecutionEnvironment.execute(JOB_TYPE);
        }

    }

    public static class FixedSource implements SourceFunction<SpeedEvent> {
        private volatile boolean running = true;


        @Override
        public void run(SourceContext<SpeedEvent> ctx) throws Exception {
            if (running)
            {
                ctx.collectWithTimestamp(new SpeedEvent("1", 1000, 55),1000);
                ctx.collectWithTimestamp(new SpeedEvent("1", 4000, 56),4000);
                ctx.collectWithTimestamp(new SpeedEvent("1", 7000, 57),7000);
                ctx.collectWithTimestamp(new SpeedEvent("1", 8000, 58),8000);
                ctx.collectWithTimestamp(new SpeedEvent("1", 10000, 60),10000);
                ctx.collectWithTimestamp(new SpeedEvent("1", 11000, 20),11000);
                ctx.collectWithTimestamp(new SpeedEvent("1", 12000, 60),12000);
                ctx.collectWithTimestamp(new SpeedEvent("1", 5000, 20),5000);
                ctx.collectWithTimestamp(new SpeedEvent("1", 9000, 20),9000);

            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
