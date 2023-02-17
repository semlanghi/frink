package windowing;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import windowing.frames.AggregateWindowing;
import windowing.frames.DeltaWindowing;
import windowing.frames.SingleBufferWindowing;
import windowing.frames.ThresholdWindowing;
import windowing.time.FrinkSlidingEventTimeWindows;
import windowing.windows.DataDrivenWindow;

import java.util.function.BiFunction;
import java.util.function.ToLongFunction;

public class ExtendedKeyedStream<T, K> extends KeyedStream<T, K> {

    public ExtendedKeyedStream(DataStream<T> dataStream, KeySelector<T, K> keySelector) {
        super(dataStream, keySelector);
    }


    public StateAwareWindowedStream timeTumbling(Long width) {
        return timeSliding(width, width);
//        return new StateAwareWindowedStream<>(this, new FrinkTumblingEventTimeWindows(width, 0));
    }

    public StateAwareWindowedStream timeSliding(Long width, Long slide) {
        return new StateAwareWindowedStream<>(this, new FrinkSlidingEventTimeWindows(width, slide, 0));
    }

    public StateAwareWindowedStream<T, K, DataDrivenWindow> frameAggregate(BiFunction<Long, Long, Long> agg, Long startValue, long threshold, ToLongFunction<T> toLongFunction) {
        return new StateAwareWindowedStream<>(this, new AggregateWindowing<>(agg, startValue, threshold, toLongFunction));
    }

    public StateAwareWindowedStream<T, K, DataDrivenWindow> frameThreshold(long threshold, ToLongFunction<T> toLongFunction) {
        return new StateAwareWindowedStream<>(this, new ThresholdWindowing<>(threshold, toLongFunction));
    }

    public StateAwareWindowedStream<T, K, DataDrivenWindow> frameDelta(long threshold, ToLongFunction<T> toLongFunction) {
        return new StateAwareWindowedStream<>(this, new DeltaWindowing<>(threshold, toLongFunction));
    }

    public StateAwareWindowedStream<T, K, GlobalWindow> frameThresholdSingle(long threshold, ToLongFunction<T> toLongFunction) {
        SingleBufferWindowing<T, GlobalWindow> singleBufferWindowing = new ThresholdWindowing<>(threshold, toLongFunction);
        return new StateAwareWindowedStream<>(this, (WindowAssigner<T, GlobalWindow>) GlobalWindows.create())
                .evictor(singleBufferWindowing.singleBufferEvictor())
                .trigger(singleBufferWindowing.singleBufferTrigger());
    }

    public StateAwareWindowedStream<T, K, GlobalWindow> frameDeltaSingle(long threshold, ToLongFunction<T> toLongFunction) {
        SingleBufferWindowing<T, GlobalWindow> singleBufferWindowing = new DeltaWindowing<>(threshold, toLongFunction);
        Evictor<T, GlobalWindow> evictor = singleBufferWindowing.singleBufferEvictor();
        return new StateAwareWindowedStream<>(this, (WindowAssigner<T, GlobalWindow>) GlobalWindows.create())
                .evictor(evictor)
                .trigger(singleBufferWindowing.singleBufferTrigger());
    }

    public StateAwareWindowedStream<T, K, GlobalWindow> frameAggregateSingle(BiFunction<Long, Long, Long> agg, Long startValue, long threshold, ToLongFunction<T> toLongFunction) {
        SingleBufferWindowing<T, GlobalWindow> singleBufferWindowing = new AggregateWindowing<>(agg, startValue, threshold, toLongFunction);
        return new StateAwareWindowedStream<>(this, (WindowAssigner<T, GlobalWindow>) GlobalWindows.create())
                .evictor(singleBufferWindowing.singleBufferEvictor())
                .trigger(singleBufferWindowing.singleBufferTrigger());
    }

    public StateAwareWindowedStream timeTumblingSingle(Long width) {
//        SingleBufferWindowing<T, GlobalWindow> singleBufferWindowing = new FrinkTumblingEventTimeWindows<>(width);//new AggregateWindowing<>(agg, startValue, threshold, toLongFunction);
//        return new StateAwareWindowedStream<>(this, (WindowAssigner<T, GlobalWindow>) GlobalWindows.create())
//                .evictor(singleBufferWindowing.singleBufferEvictor())
//                .trigger(singleBufferWindowing.singleBufferTrigger());
        return timeSlidingSingle(width, width);
    }

    public StateAwareWindowedStream timeSlidingSingle(Long width, Long slide) {
        SingleBufferWindowing<T, GlobalWindow> singleBufferWindowing = new FrinkSlidingEventTimeWindows<>(width, slide);
        Trigger<T, GlobalWindow> trigger = singleBufferWindowing.singleBufferTrigger();
        Evictor<T, GlobalWindow> evictor = singleBufferWindowing.singleBufferEvictor();
        return new StateAwareWindowedStream<>(this, (WindowAssigner<T, GlobalWindow>) GlobalWindows.create())
                .evictor(evictor)
                .trigger(trigger);

    }

}
