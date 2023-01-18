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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.shaded.guava18.com.google.common.base.Function;
import org.apache.flink.shaded.guava18.com.google.common.collect.FluentIterable;
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
import windowing.frames.FrameWindowing;
import windowing.windows.DataDrivenWindow;

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

    private final FrameWindowing<IN>.FrameTrigger frameTrigger;

    private final StateDescriptor<? extends ListState<StreamRecord<IN>>, ?> evictingWindowStateDescriptor;

    // ------------------------------------------------------------------------
    // the fields below are instantiated once the operator runs in the runtime

    private transient EvictorContext evictorContext;

    private transient InternalListState<K, W, StreamRecord<IN>> evictingWindowState;

    // ------------------------------------------------------------------------

    // A stream to report timestamps of processing elements
    final OutputTag<String> latencyStream = new OutputTag<String>("latency"){};
    final OutputTag<String> stateSizeStream = new OutputTag<String>("stateSize"){};

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

        //Unchecked assignment, but assumed to be a frame trigger for now
        this.frameTrigger = (FrameWindowing<IN>.FrameTrigger) trigger;
        this.evictor = checkNotNull(evictor);
        this.evictingWindowStateDescriptor = checkNotNull(windowStateDescriptor);
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        //This operation is not really necessary, as the window returned is always the GlobalWindow
        final Collection<W> elementWindows = windowAssigner.assignWindows(
                element.getValue(), element.getTimestamp(), windowAssignerContext);

        final K key = this.<K>getKeyedStateBackend().getCurrentKey();

        if (element.getTimestamp() + allowedLateness <= internalTimerService.currentWatermark())
            return;

        // No check on the merging window assigner (WA), being single buffer it uses only GlobalWindows as the WA
        // N.B.: Always one window, i.e., the Global Window
        W window = elementWindows.iterator().next();
        StringBuilder latency = new StringBuilder();
        StringBuilder stateSize = new StringBuilder();
        //TODO: SB ADD Start
        latency.append("SB_ADD_Start="+System.nanoTime()+",");
        evictingWindowState.setCurrentNamespace(window);
        evictingWindowState.add(element);

        //TODO: SB ADD End
        latency.append("SB_ADD_End="+System.nanoTime()+",");
        triggerContext.key = key;
        triggerContext.window = window;
        evictorContext.key = key;
        evictorContext.window = window;


        ComplexTriggerResult complexTriggerResult = this.frameTrigger.onWindow(element.getValue(), element.getTimestamp(), triggerContext);

        //TODO: SB Content Start
        latency.append("SB_Content_Start="+System.nanoTime()+",");
        Iterable<StreamRecord<IN>> contents = evictingWindowState.get();
        if (contents == null) {
            // if we have no state, there is nothing to do
            return;
        }

        // Work around type system restrictions...
        FluentIterable<TimestampedValue<IN>> recordsWithTimestamp = FluentIterable
                .from(contents)
                .transform(new Function<StreamRecord<IN>, TimestampedValue<IN>>() {
                    @Override
                    public TimestampedValue<IN> apply(StreamRecord<IN> input) {
                        return TimestampedValue.from(input);
                    }
                });

        SortedMap<DataDrivenWindow, ? extends Iterable<TimestampedValue<IN>>> iterables =
                extractData(recordsWithTimestamp, complexTriggerResult.resultWindows);
        //TODO: SB Content End
        latency.append("SB_Content_End="+System.nanoTime()+",");
        //Ahmed: Will do the iteration to compute the state here to avoid affecting the latency of content delivery
        long count =0;
        for (Object obj : contents)
        {
            count++;
        }
        stateSize.append("stateSize="+count+",");
        // The eviction before the emission of the output is not necessary

        for (DataDrivenWindow tmp : complexTriggerResult.resultWindows) {
            if (complexTriggerResult.internalResult.isFire()) {
                if(iterables.containsKey(tmp))
                    emitWindowContents(window, iterables.get(tmp), evictingWindowState);
            }
        }

        // The evictor should take the collections of events, but also the size of the collection
        // this is not usefult for a time-based eviction. Thus, we pass instead of the size, the allowed lateness

        //TODO: SB Evict Start
        latency.append("SB_Evict_Start="+System.nanoTime()+",");
        evictorContext.evictAfter(recordsWithTimestamp, (int) (this.allowedLateness/1000));
        //TODO: SB Evict End
        latency.append("SB_Evict_End="+System.nanoTime());
        //Emit to the side output stream
        this.output.collect(latencyStream,new StreamRecord<String>(latency.toString()));
        this.output.collect(stateSizeStream,new StreamRecord<String>(stateSize.toString()));
    }

    /**
     * This method extracts the related events given a set of windows from the State Backend.
     * @param resultCollection
     * @param resultWindows
     * @return a map object containing the windows mapped to the related set of events
     */
    private SortedMap<DataDrivenWindow, ? extends Iterable<TimestampedValue<IN>>> extractData(Iterable<TimestampedValue<IN>> resultCollection, Collection<DataDrivenWindow> resultWindows){

        SortedMap<DataDrivenWindow, List<TimestampedValue<IN>>> internalWindows = new TreeMap<>(Comparator.comparingLong(DataDrivenWindow::getEnd));
        resultCollection.forEach(inTimestampedValue -> resultWindows.stream()
                .filter(window -> window.getStart()<=inTimestampedValue.getTimestamp() && window.getEnd()> inTimestampedValue.getTimestamp())
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

        void evictAfter(Iterable<TimestampedValue<IN>>  elements, int allowedLateness) {
            evictor.evictAfter((Iterable) elements, allowedLateness, window, this);
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
                evictingWindowState.setCurrentNamespace(this.window);
                return StateAwareSingleBufferWindowOperator.this.evictingWindowState.get();
            } catch (Exception e) {
                e.printStackTrace();
            } return null;
        }

        public long getAllowedLateness(){
            return allowedLateness;
        }


        /**
         * NB: I will use this method to check the trigger on the window, NOT on the processing time
         * Artificial Solution, but doable for now
         * @param time
         * @return
         * @throws Exception
         */
        @Override
        public TriggerResult onProcessingTime(long time) throws Exception {
            return StateAwareSingleBufferWindowOperator.this.trigger.onProcessingTime(time, window, this);
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
