/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package windowing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.shaded.guava18.com.google.common.base.Function;
import org.apache.flink.shaded.guava18.com.google.common.base.Predicate;
import org.apache.flink.shaded.guava18.com.google.common.collect.FluentIterable;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.MergingWindowSet;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;
import windowing.frames.FrameState;
import windowing.windows.CandidateTimeWindow;

import javax.annotation.Nullable;
import java.util.*;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link StateAwareMultiBufferWindowOperator} that also allows an {@link Evictor} to be used.
 *
 * <p>The {@code Evictor} is used to remove elements from a pane before/after the evaluation of
 * {@link InternalWindowFunction} and after the window evaluation gets triggered by a
 * {@link org.apache.flink.streaming.api.windowing.triggers.Trigger}.
 *
 * @param <K> The type of key returned by the {@code KeySelector}.
 * @param <IN> The type of the incoming elements.
 * @param <OUT> The type of elements emitted by the {@code InternalWindowFunction}.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
@Internal
public class StateAwareSingleBufferWindowOperator<K, IN, OUT, W extends Window>
        extends StateAwareMultiBufferWindowOperator<K, IN, Iterable<IN>, OUT, W> {

    private static final long serialVersionUID = 1L;

    // ------------------------------------------------------------------------
    // these fields are set by the API stream graph builder to configure the operator

    private final Evictor<? super IN, ? super W> evictor;

    private final StateDescriptor<? extends ListState<StreamRecord<IN>>, ?> evictingWindowStateDescriptor;

    // ------------------------------------------------------------------------
    // the fields below are instantiated once the operator runs in the runtime

    private transient EvictorContext evictorContext;

    private transient InternalListState<K, W, StreamRecord<IN>> evictingWindowState;

    // ------------------------------------------------------------------------

    public StateAwareSingleBufferWindowOperator(WindowAssigner<? super IN, W> windowAssigner,
                                                TypeSerializer<W> windowSerializer,
                                                KeySelector<IN, K> keySelector,
                                                TypeSerializer<K> keySerializer,
                                                StateDescriptor<? extends ListState<StreamRecord<IN>>, ?> windowStateDescriptor,
                                                InternalWindowFunction<Iterable<IN>, OUT, K, W> windowFunction,
                                                Trigger<? super IN, ? super W> trigger,
                                                Evictor<? super IN, ? super W> evictor,
                                                long allowedLateness,
                                                OutputTag<IN> lateDataOutputTag) {

        super(windowAssigner, windowSerializer, keySelector,
                keySerializer, null, windowFunction, trigger, allowedLateness, lateDataOutputTag);

        this.evictor = checkNotNull(evictor);
        this.evictingWindowStateDescriptor = checkNotNull(windowStateDescriptor);
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        final Collection<W> elementWindows = windowAssigner.assignWindows(
                element.getValue(), element.getTimestamp(), windowAssignerContext);

        //if element is handled by none of assigned elementWindows
        boolean isSkippedElement = true;

        final K key = this.<K>getKeyedStateBackend().getCurrentKey();

        if (windowAssigner instanceof MergingWindowAssigner) {
            MergingWindowSet<W> mergingWindows = getMergingWindowSet();

            for (W window : elementWindows) {

                // adding the new window might result in a merge, in that case the actualWindow
                // is the merged window and we work with that. If we don't merge then
                // actualWindow == window
                W actualWindow = mergingWindows.addWindow(window,
                        new MergingWindowSet.MergeFunction<W>() {
                            @Override
                            public void merge(W mergeResult,
                                              Collection<W> mergedWindows, W stateWindowResult,
                                              Collection<W> mergedStateWindows) throws Exception {

                                if ((windowAssigner.isEventTime() && mergeResult.maxTimestamp() + allowedLateness <= internalTimerService.currentWatermark())) {
                                    throw new UnsupportedOperationException("The end timestamp of an " +
                                            "event-time window cannot become earlier than the current watermark " +
                                            "by merging. Current watermark: " + internalTimerService.currentWatermark() +
                                            " window: " + mergeResult);
                                } else if (!windowAssigner.isEventTime() && mergeResult.maxTimestamp() <= internalTimerService.currentProcessingTime()) {
                                    throw new UnsupportedOperationException("The end timestamp of a " +
                                            "processing-time window cannot become earlier than the current processing time " +
                                            "by merging. Current processing time: " + internalTimerService.currentProcessingTime() +
                                            " window: " + mergeResult);
                                }

                                triggerContext.key = key;
                                triggerContext.window = mergeResult;

                                triggerContext.onMerge(mergedWindows);

                                for (W m : mergedWindows) {
                                    triggerContext.window = m;
                                    triggerContext.clear();
                                    deleteCleanupTimer(m);
                                }

                                // merge the merged state windows into the newly resulting state window
                                evictingWindowState.mergeNamespaces(stateWindowResult, mergedStateWindows);
                            }
                        });

                // drop if the window is already late
                if (isWindowLate(actualWindow)) {
                    mergingWindows.retireWindow(actualWindow);
                    continue;
                }
                isSkippedElement = false;

                W stateWindow = mergingWindows.getStateWindow(actualWindow);
                if (stateWindow == null) {
                    throw new IllegalStateException("Window " + window + " is not in in-flight window set.");
                }

                evictingWindowState.setCurrentNamespace(stateWindow);
                evictingWindowState.add(element);

                triggerContext.key = key;
                triggerContext.window = actualWindow;
                evictorContext.key = key;
                evictorContext.window = actualWindow;

                TriggerResult triggerResult = triggerContext.onElement(element);

                Iterable<StreamRecord<IN>> contents = evictingWindowState.get();
                if (contents == null) {
                    // if we have no state, there is nothing to do
                    continue;
                }

                timestampedCollector.setAbsoluteTimestamp(window.maxTimestamp());

                // Work around type system restrictions...
                FluentIterable<TimestampedValue<IN>> recordsWithTimestamp = FluentIterable
                        .from(contents)
                        .transform(input -> TimestampedValue.from(input));

                Map<CandidateTimeWindow, Iterable<TimestampedValue<IN>>> iterables =
                        divideGlobalWindow(recordsWithTimestamp, getPartitionedState(new MapStateDescriptor<>("candidateWindows", new LongSerializer(), new CandidateTimeWindow.Serializer())));

                for (CandidateTimeWindow tmp : iterables.keySet()
                ) {
                    if (triggerResult.isFire()) {
                        emitWindowContents(window, iterables.get(tmp), evictingWindowState);

                        //work around to fix FLINK-4369, remove the evicted elements from the windowState.
                        //this is inefficient, but there is no other way to remove elements from ListState, which is an AppendingState.
                        evictingWindowState.clear();
                        for (TimestampedValue<IN> record : recordsWithTimestamp) {
                            evictingWindowState.add(record.getStreamRecord());
                        }
                    }

                    if (triggerResult.isPurge()) {
                        evictingWindowState.clear();
                    }
                    registerCleanupTimer(window);
                }
            }

            // need to make sure to update the merging state in state
            mergingWindows.persist();
        } else {
            for (W window : elementWindows) {

                // check if the window is already inactive
                if (isWindowLate(window)) {
                    continue;
                }
                isSkippedElement = false;

                evictingWindowState.setCurrentNamespace(window);
                evictingWindowState.add(element);

                triggerContext.key = key;
                triggerContext.window = window;
                evictorContext.key = key;
                evictorContext.window = window;

                TriggerResult triggerResult = triggerContext.onElement(element);

                Iterable<StreamRecord<IN>> contents = evictingWindowState.get();
                if (contents == null) {
                    // if we have no state, there is nothing to do
                    continue;
                }
                
                timestampedCollector.setAbsoluteTimestamp(window.maxTimestamp());

                // Work around type system restrictions...
                FluentIterable<TimestampedValue<IN>> recordsWithTimestamp = FluentIterable
                        .from(contents)
                        .transform(new Function<StreamRecord<IN>, TimestampedValue<IN>>() {
                            @Override
                            public TimestampedValue<IN> apply(StreamRecord<IN> input) {
                                return TimestampedValue.from(input);
                            }
                        });
                
                Map<CandidateTimeWindow, Iterable<TimestampedValue<IN>>> iterables =
                        divideGlobalWindow(recordsWithTimestamp, getPartitionedState(window, windowSerializer, new MapStateDescriptor<>("candidateWindows", new LongSerializer(), new CandidateTimeWindow.Serializer())));
                Iterable<TimestampedValue<IN>> resultAfterEviction = new ArrayList<>();
                FluentIterable<TimestampedValue<IN>> fluentResults = FluentIterable.from(resultAfterEviction);


                for (CandidateTimeWindow tmp : iterables.keySet()
                     ) {
                    if (triggerResult.isFire()) {
                        emitWindowContents(window, iterables.get(tmp), evictingWindowState);

                        //work around to fix FLINK-4369, remove the evicted elements from the windowState.
                        //this is inefficient, but there is no other way to remove elements from ListState, which is an AppendingState.
                        if(!tmp.isClosed()){
                            fluentResults = fluentResults.append(iterables.get(tmp));
                        }else{
                            getPartitionedState(window, windowSerializer, new MapStateDescriptor<>("candidateWindows", new LongSerializer(), new CandidateTimeWindow.Serializer())).remove(tmp.getStart());
                        }

                    }
                }

                evictingWindowState.clear();
                for (TimestampedValue<IN> record : fluentResults) {
                    evictingWindowState.add(record.getStreamRecord());
                }

                if (triggerResult.isPurge()) {
                    evictingWindowState.clear();
                }
                registerCleanupTimer(window);

            }
        }

        // side output input event if
        // element not handled by any window
        // late arriving tag has been set
        // windowAssigner is event time and current timestamp + allowed lateness no less than element timestamp
        if (isSkippedElement && isElementLate(element)) {
            if (lateDataOutputTag != null){
                sideOutput(element);
            } else {
                this.numLateRecordsDropped.inc();
            }
        }
    }

    private Map<CandidateTimeWindow, Iterable<TimestampedValue<IN>>> divideGlobalWindow(Iterable<TimestampedValue<IN>> collection, MapState<Long, CandidateTimeWindow> mapState){
        try {
            List<CandidateTimeWindow> timeWindows = new ArrayList<>();
            Map<CandidateTimeWindow, Iterable<TimestampedValue<IN>>> internalWindows = new HashMap<>();

            if(mapState.keys()==null)
                return Collections.emptyMap();

            for (CandidateTimeWindow timeWindow : mapState.values()) {
                FluentIterable<TimestampedValue<IN>> fluentIterable = FluentIterable
                        .from(collection)
                        .filter(new Predicate<TimestampedValue<IN>>() {
                            @Override
                            public boolean apply(@Nullable TimestampedValue<IN> inStreamRecord) {
                                assert inStreamRecord != null;
                                return inStreamRecord.getTimestamp() >= timeWindow.getStart() && inStreamRecord.getTimestamp() <= timeWindow.getEnd() - 1;
                            }
                        });
                internalWindows.put(timeWindow, fluentIterable);
            } return internalWindows;
        } catch (Exception e) {
            e.printStackTrace();
        } return Collections.emptyMap();
    }

    @Override
    public void onEventTime(InternalTimer<K, W> timer) throws Exception {

        triggerContext.key = timer.getKey();
        triggerContext.window = timer.getNamespace();
        evictorContext.key = timer.getKey();
        evictorContext.window = timer.getNamespace();

        MergingWindowSet<W> mergingWindows = null;

        if (windowAssigner instanceof MergingWindowAssigner) {
            mergingWindows = getMergingWindowSet();
            W stateWindow = mergingWindows.getStateWindow(triggerContext.window);
            if (stateWindow == null) {
                // Timer firing for non-existent window, this can only happen if a
                // trigger did not clean up timers. We have already cleared the merging
                // window and therefore the Trigger state, however, so nothing to do.
                return;
            } else {
                evictingWindowState.setCurrentNamespace(stateWindow);
            }
        } else {
            evictingWindowState.setCurrentNamespace(triggerContext.window);
        }

        TriggerResult triggerResult = triggerContext.onEventTime(timer.getTimestamp());

        if (triggerResult.isFire()) {
            Iterable<StreamRecord<IN>> contents = evictingWindowState.get();
            if (contents != null) {
                emitWindowContents(triggerContext.window, null, evictingWindowState);
            }
        }

        if (triggerResult.isPurge()) {
            evictingWindowState.clear();
        }

        if (windowAssigner.isEventTime() && isCleanupTime(triggerContext.window, timer.getTimestamp())) {
            clearAllState(triggerContext.window, evictingWindowState, mergingWindows);
        }

        if (mergingWindows != null) {
            // need to make sure to update the merging state in state
            mergingWindows.persist();
        }
    }

    @Override
    public void onProcessingTime(InternalTimer<K, W> timer) throws Exception {
        triggerContext.key = timer.getKey();
        triggerContext.window = timer.getNamespace();
        evictorContext.key = timer.getKey();
        evictorContext.window = timer.getNamespace();

        MergingWindowSet<W> mergingWindows = null;

        if (windowAssigner instanceof MergingWindowAssigner) {
            mergingWindows = getMergingWindowSet();
            W stateWindow = mergingWindows.getStateWindow(triggerContext.window);
            if (stateWindow == null) {
                // Timer firing for non-existent window, this can only happen if a
                // trigger did not clean up timers. We have already cleared the merging
                // window and therefore the Trigger state, however, so nothing to do.
                return;
            } else {
                evictingWindowState.setCurrentNamespace(stateWindow);
            }
        } else {
            evictingWindowState.setCurrentNamespace(triggerContext.window);
        }

        TriggerResult triggerResult = triggerContext.onProcessingTime(timer.getTimestamp());

        if (triggerResult.isFire()) {
            Iterable<StreamRecord<IN>> contents = evictingWindowState.get();
            if (contents != null) {
                emitWindowContents(triggerContext.window, null, evictingWindowState);
            }
        }

        if (triggerResult.isPurge()) {
            evictingWindowState.clear();
        }

        if (!windowAssigner.isEventTime() && isCleanupTime(triggerContext.window, timer.getTimestamp())) {
            clearAllState(triggerContext.window, evictingWindowState, mergingWindows);
        }

        if (mergingWindows != null) {
            // need to make sure to update the merging state in state
            mergingWindows.persist();
        }
    }

    private void emitWindowContents(W window, Iterable<TimestampedValue<IN>> contents, ListState<StreamRecord<IN>> windowState) throws Exception {

        FluentIterable<TimestampedValue<IN>> fluentIterable = FluentIterable.from(contents);

        evictorContext.evictBefore(fluentIterable, Iterables.size(contents));



        FluentIterable<IN> projectedContents = fluentIterable
                .transform(new Function<TimestampedValue<IN>, IN>() {
                    @Override
                    public IN apply(TimestampedValue<IN> input) {
                        if(input.getValue()==null)
                            System.out.println("cjsddnasd");
                        return input.getValue();
                    }
                });

        processContext.window = triggerContext.window;
        userFunction.process(triggerContext.key, triggerContext.window, processContext, projectedContents, timestampedCollector);
//        evictorContext.evictAfter(fluentIterable, Iterables.size(contents));
    }

    private void clearAllState(
            W window,
            ListState<StreamRecord<IN>> windowState,
            MergingWindowSet<W> mergingWindows) throws Exception {
        windowState.clear();
        triggerContext.clear();
        processContext.window = window;
        processContext.clear();
        if (mergingWindows != null) {
            mergingWindows.retireWindow(window);
            mergingWindows.persist();
        }
    }

    /**
     * {@code EvictorContext} is a utility for handling {@code Evictor} invocations. It can be reused
     * by setting the {@code key} and {@code window} fields. No internal state must be kept in
     * the {@code EvictorContext}.
     */

    class EvictorContext implements Evictor.EvictorContext {

        protected K key;
        protected W window;

        public EvictorContext(K key, W window) {
            this.key = key;
            this.window = window;
        }

        @Override
        public long getCurrentProcessingTime() {
            return internalTimerService.currentProcessingTime();
        }

        @Override
        public long getCurrentWatermark() {
            return internalTimerService.currentWatermark();
        }

        @Override
        public MetricGroup getMetricGroup() {
            return StateAwareSingleBufferWindowOperator.this.getMetricGroup();
        }

        public K getKey() {
            return key;
        }

        void evictBefore(Iterable<TimestampedValue<IN>> elements, int size) {
            evictor.evictBefore((Iterable) elements, size, window, this);
        }

        void evictAfter(Iterable<TimestampedValue<IN>>  elements, int size) {
            evictor.evictAfter((Iterable) elements, size, window, this);
        }
    }

    @Override
    public void open() throws Exception {
        super.open();

        triggerContext = new StateAwareContext(null, null);
        evictorContext = new EvictorContext(null, null);
        evictingWindowState = (InternalListState<K, W, StreamRecord<IN>>)
                getOrCreateKeyedState(windowSerializer, evictingWindowStateDescriptor);
    }

    protected class StateAwareContext extends Context{

        public StateAwareContext(K key, W window) {
            super(key, window);
        }

        public ValueState<FrameState> getCurrentFrameState(ValueStateDescriptor<FrameState> stateDescriptor) {
            try {
                return getPartitionedState(stateDescriptor);
            } catch (Exception e) {
                e.printStackTrace();
            } return null;
        }

        public MapState<Long, FrameState> getPastFrameState(MapStateDescriptor<Long, FrameState> stateDescriptor) {
            try {
                return getPartitionedState(stateDescriptor);
            } catch (Exception e) {
                e.printStackTrace();
            } return null;
        }

        public Iterable<StreamRecord<IN>> getContent(W window) {
            try {
                W stateWindow = getSplittingMergingWindowSet().getStateWindow(window);
                evictingWindowState.setCurrentNamespace(stateWindow);
                return StateAwareSingleBufferWindowOperator.this.evictingWindowState.get();
            } catch (Exception e) {
                e.printStackTrace();
            } return null;
        }

    }

    @Override
    public void close() throws Exception {
        super.close();
        evictorContext = null;
    }

    @Override
    public void dispose() throws Exception{
        super.dispose();
        evictorContext = null;
    }

    // ------------------------------------------------------------------------
    // Getters for testing
    // ------------------------------------------------------------------------

    @VisibleForTesting
    public Evictor<? super IN, ? super W> getEvictor() {
        return evictor;
    }

    @Override
    @VisibleForTesting
    @SuppressWarnings("unchecked, rawtypes")
    public StateDescriptor<? extends AppendingState<StreamRecord<IN>, Iterable<StreamRecord<IN>>>, ?> getStateDescriptor() {
        return evictingWindowStateDescriptor;
    }
}
