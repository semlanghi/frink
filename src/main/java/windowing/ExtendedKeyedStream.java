package windowing;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import windowing.frames.AggregateWindowing;
import windowing.frames.DeltaWindowing;
import windowing.frames.ThresholdWindowing;

import java.util.function.BiFunction;
import java.util.function.ToLongFunction;

public class ExtendedKeyedStream<T,K> extends KeyedStream<T,K> {

    public ExtendedKeyedStream(DataStream<T> dataStream, KeySelector<T, K> keySelector) {
        super(dataStream, keySelector);
    }

    public StateAwareWindowedStream<T,K, TimeWindow> frameAggregate(BiFunction<Long,Long,Long> agg, Long startValue, long threshold, ToLongFunction<T> toLongFunction){
        return new StateAwareWindowedStream<>(this, new AggregateWindowing<>(agg, startValue, threshold, toLongFunction));
    }

    public StateAwareWindowedStream<T,K, TimeWindow> frameThreshold(long threshold, ToLongFunction<T> toLongFunction){
        return new StateAwareWindowedStream<>(this, new ThresholdWindowing<>(threshold, toLongFunction));
    }

    public StateAwareWindowedStream<T,K, TimeWindow> frameDelta(long threshold, ToLongFunction<T> toLongFunction){
        return new StateAwareWindowedStream<>(this, new DeltaWindowing<>(threshold, toLongFunction));
    }

    public StateAwareWindowedStream<T,K, GlobalWindow> frameThresholdSingle(long threshold, ToLongFunction<T> toLongFunction){
        SingleBufferWindowing<T, GlobalWindow> singleBufferWindowing = new ThresholdWindowing<>(threshold, toLongFunction);
        return new StateAwareWindowedStream<>(this, GlobalWindows.create())
                .evictor(singleBufferWindowing.evictor())
                .trigger(singleBufferWindowing.singleBufferTrigger());
    }

    public StateAwareWindowedStream<T,K, GlobalWindow> frameDeltaSingle(long threshold, ToLongFunction<T> toLongFunction){
        SingleBufferWindowing<T, GlobalWindow> singleBufferWindowing = new DeltaWindowing<>(threshold, toLongFunction);
        return new StateAwareWindowedStream<>(this, GlobalWindows.create())
                .evictor(singleBufferWindowing.evictor())
                .trigger(singleBufferWindowing.singleBufferTrigger());
    }

    public StateAwareWindowedStream<T,K, GlobalWindow> frameAggregateSingle(BiFunction<Long,Long,Long> agg, Long startValue, long threshold, ToLongFunction<T> toLongFunction){
        SingleBufferWindowing<T, GlobalWindow> singleBufferWindowing = new AggregateWindowing<>(agg, startValue, threshold, toLongFunction);
        return new StateAwareWindowedStream<>(this, GlobalWindows.create())
                .evictor(singleBufferWindowing.evictor())
                .trigger(singleBufferWindowing.singleBufferTrigger());
    }



}
