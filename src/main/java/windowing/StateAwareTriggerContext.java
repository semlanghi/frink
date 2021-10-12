package windowing;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.internal.InternalMergingState;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import windowing.frames.FrameState;

import java.io.Serializable;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@code Context} is a utility for handling {@code Trigger} invocations. It can be reused
 * by setting the {@code key} and {@code window} fields. No internal state must be kept in
 * the {@code Context}
 */
public abstract class StateAwareTriggerContext<K,T,W extends Window>{
    public K key;
    public W window;
    protected Trigger<T,W> trigger;
    protected InternalTimerService<W> internalTimerService;

    protected Collection<W> mergedWindows;

    public StateAwareTriggerContext(K key, W window, Trigger<T, W> trigger, InternalTimerService<W> internalTimerService,
                                     MetricGroup metricGroup, TypeSerializer<T> typeSerializer) {

    }

//    @Override
//    public abstract MetricGroup getMetricGroup();
//
//    public long getCurrentWatermark() {
//        return internalTimerService.currentWatermark();
//    }
//
//    @Override
//    public <S extends Serializable> ValueState<S> getKeyValueState(String name,
//                                                                   Class<S> stateType,
//                                                                   S defaultState) {
//        checkNotNull(stateType, "The state type class must not be null");
//
//        TypeInformation<S> typeInfo;
//        try {
//            typeInfo = TypeExtractor.getForClass(stateType);
//        }
//        catch (Exception e) {
//            throw new RuntimeException("Cannot analyze type '" + stateType.getName() +
//                    "' from the class alone, due to generic type parameters. " +
//                    "Please specify the TypeInformation directly.", e);
//        }
//
//        return getKeyValueState(name, typeInfo, defaultState);
//    }
//
//    @Override
//    public abstract <S extends Serializable> ValueState<S> getKeyValueState(String name,
//                                                                   TypeInformation<S> stateType,
//                                                                   S defaultState);
//
//    @Override
//    public <S extends MergingState<?, ?>> void mergePartitionedState(StateDescriptor<S, ?> stateDescriptor) {
//        if (mergedWindows != null && mergedWindows.size() > 0) {
//            try {
//                S rawState = getKeyedStateBackend().getOrCreateKeyedState(typeSerializer, stateDescriptor);
//
//                if (rawState instanceof InternalMergingState) {
//                    @SuppressWarnings("unchecked")
//                    InternalMergingState<K, W, ?, ?, ?> mergingState = (InternalMergingState<K, W, ?, ?, ?>) rawState;
//                    mergingState.mergeNamespaces(window, mergedWindows);
//                }
//                else {
//                    throw new IllegalArgumentException(
//                            "The given state descriptor does not refer to a mergeable state (MergingState)");
//                }
//            }
//            catch (Exception e) {
//                throw new RuntimeException("Error while merging state.", e);
//            }
//        }
//    }
//
//    protected abstract <K> KeyedStateBackend<K> getKeyedStateBackend();
//
//    @Override
//    public long getCurrentProcessingTime() {
//        return internalTimerService.currentProcessingTime();
//    }
//
//    @Override
//    public void registerProcessingTimeTimer(long time) {
//        internalTimerService.registerProcessingTimeTimer(window, time);
//    }
//
//    @Override
//    public void registerEventTimeTimer(long time) {
//        internalTimerService.registerEventTimeTimer(window, time);
//    }
//
//    @Override
//    public void deleteProcessingTimeTimer(long time) {
//        internalTimerService.deleteProcessingTimeTimer(window, time);
//    }
//
//    @Override
//    public void deleteEventTimeTimer(long time) {
//        internalTimerService.deleteEventTimeTimer(window, time);
//    }
//
//    public TriggerResult onElement(StreamRecord<T> element) throws Exception {
//        return trigger.onElement(element.getValue(), element.getTimestamp(), window, this);
//    }
//
//    public TriggerResult onProcessingTime(long time) throws Exception {
//        return trigger.onProcessingTime(time, window, this);
//    }
//
//    public TriggerResult onEventTime(long time) throws Exception {
//        return trigger.onEventTime(time, window, this);
//    }
//
//    public void onMerge(Collection<W> mergedWindows) throws Exception {
//        this.mergedWindows = mergedWindows;
//        trigger.onMerge(window, this);
//    }
//
//    public void clear() throws Exception {
//        trigger.clear(window, this);
//    }
//
//    public abstract ValueState<FrameState> getCurrentFrameState(ValueStateDescriptor<FrameState> stateDescriptor);
//
//    public abstract MapState<Long, FrameState> getPastFrameState(MapStateDescriptor<Long, FrameState> stateDescriptor);
//
//    public abstract Iterable<StreamRecord<T>> getContent(W window);

    @Override
    public String toString() {
        return "Context{" +
                "key=" + key +
                ", window=" + window +
                '}';
    }
}
