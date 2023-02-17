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

package windowing.time;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.AppendingState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.shaded.guava18.com.google.common.base.Function;
import org.apache.flink.shaded.guava18.com.google.common.collect.FluentIterable;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.MergingWindowSet;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;
import windowing.ComplexTriggerResult;

import java.sql.Time;
import java.util.*;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link WindowOperator} that also allows an {@link Evictor} to be used.
 *
 * <p>The {@code Evictor} is used to remove elements from a pane before/after the evaluation of
 * {@link InternalWindowFunction} and after the window evaluation gets triggered by a
 * {@link Trigger}.
 *
 * @param <K>   The type of key returned by the {@code KeySelector}.
 * @param <IN>  The type of the incoming elements.
 * @param <OUT> The type of elements emitted by the {@code InternalWindowFunction}.
 * @param <W>   The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
@Internal
public class FrinkTimeBasedEvictingWindowOperator<K, IN, OUT, W extends Window>
        extends FrinkTimeBasedMultiBufferWindowOperator<K, IN, Iterable<IN>, OUT, W> {

    private static final long serialVersionUID = 1L;

    private final CustomTrigger<IN, TimeWindow> singleBufferTrigger;

    // ------------------------------------------------------------------------
    // these fields are set by the API stream graph builder to configure the operator

    private final Evictor<? super IN, ? super W> evictor;

    private final StateDescriptor<? extends ListState<StreamRecord<IN>>, ?> evictingWindowStateDescriptor;

    // ------------------------------------------------------------------------
    // the fields below are instantiated once the operator runs in the runtime

    private transient EvictorContext evictorContext;

    private transient InternalListState<K, W, StreamRecord<IN>> evictingWindowState;

    // ------------------------------------------------------------------------

    public FrinkTimeBasedEvictingWindowOperator(WindowAssigner<? super IN, W> windowAssigner,
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

        this.singleBufferTrigger = (CustomTrigger<IN, TimeWindow>) trigger;

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

//            TriggerResult triggerResult = triggerContext.onElement(element);
            ComplexTriggerResult<TimeWindow> complexTriggerResult = this.singleBufferTrigger.onWindow(element.getValue(), element.getTimestamp(), triggerContext, windowAssignerContext);
            Iterable<StreamRecord<IN>> contents = evictingWindowState.get();

            if (contents == null) {
                continue;
            }

            long count = 0;
            for (Object obj : contents) {
                count++;
            }

            SortedMap<TimeWindow, ? extends Iterable<StreamRecord<IN>>> win2Fire = extractData(contents, complexTriggerResult.resultWindows);


            for (TimeWindow w : complexTriggerResult.resultWindows) {
                //actually a single window
                if (complexTriggerResult.internalResult.isFire()) {

                    if (win2Fire.containsKey(w))
                        emitWindowContents(window, win2Fire.get(w), evictingWindowState);
                }
                if (complexTriggerResult.internalResult.isPurge()) {
                    evictingWindowState.clear();
                }
            }

            registerCleanupTimer(window);
        }


        // side output input event if
        // element not handled by any window
        // late arriving tag has been set
        // windowAssigner is event time and current timestamp + allowed lateness no less than element timestamp
        if (isSkippedElement && isElementLate(element)) {
            if (lateDataOutputTag != null) {
                sideOutput(element);
            } else {
                this.numLateRecordsDropped.inc();
            }
        }
    }

    private SortedMap<TimeWindow, ? extends Iterable<StreamRecord<IN>>> extractData(Iterable<StreamRecord<IN>> resultCollection, Collection<TimeWindow> resultWindows) {

        SortedMap<TimeWindow, List<StreamRecord<IN>>> internalWindows = new TreeMap<>(Comparator.comparingLong(TimeWindow::getEnd));
        resultCollection.forEach(inTimestampedValue -> resultWindows.stream()
                .filter(window -> window.getStart() <= inTimestampedValue.getTimestamp() && window.maxTimestamp() + 1 > inTimestampedValue.getTimestamp())
                .findFirst()
                .ifPresent(window -> {
                    internalWindows.putIfAbsent(window, new ArrayList<>());
                    internalWindows.get(window).add(inTimestampedValue);
                }));

        return internalWindows;
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
                emitWindowContents(triggerContext.window, contents, evictingWindowState);
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
                emitWindowContents(triggerContext.window, contents, evictingWindowState);
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

    private void emitWindowContents(W window, Iterable<StreamRecord<IN>> contents, ListState<StreamRecord<IN>> windowState) throws Exception {
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
        evictorContext.evictBefore(recordsWithTimestamp, Iterables.size(recordsWithTimestamp));

        FluentIterable<IN> projectedContents = recordsWithTimestamp
                .transform(new Function<TimestampedValue<IN>, IN>() {
                    @Override
                    public IN apply(TimestampedValue<IN> input) {
                        return input.getValue();
                    }
                });

        processContext.window = triggerContext.window;
        userFunction.process(triggerContext.key, triggerContext.window, processContext, projectedContents, timestampedCollector);
        evictorContext.evictAfter(recordsWithTimestamp, Iterables.size(recordsWithTimestamp));

        //work around to fix FLINK-4369, remove the evicted elements from the windowState.
        //this is inefficient, but there is no other way to remove elements from ListState, which is an AppendingState.
        windowState.clear();
        for (TimestampedValue<IN> record : recordsWithTimestamp) {
            windowState.add(record.getStreamRecord());
        }
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
            return FrinkTimeBasedEvictingWindowOperator.this.getMetricGroup();
        }

        public K getKey() {
            return key;
        }

        void evictBefore(Iterable<TimestampedValue<IN>> elements, int size) {
            evictor.evictBefore((Iterable) elements, size, window, this);
        }

        void evictAfter(Iterable<TimestampedValue<IN>> elements, int size) {
            evictor.evictAfter((Iterable) elements, size, window, this);
        }
    }

    @Override
    public void open() throws Exception {
        super.open();

        evictorContext = new EvictorContext(null, null);
        evictingWindowState = (InternalListState<K, W, StreamRecord<IN>>)
                getOrCreateKeyedState(windowSerializer, evictingWindowStateDescriptor);
    }

    @Override
    public void close() throws Exception {
        super.close();
        evictorContext = null;
    }

    @Override
    public void dispose() throws Exception {
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