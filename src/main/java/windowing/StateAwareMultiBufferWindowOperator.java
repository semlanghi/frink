/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package windowing;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.DefaultKeyedStateStore;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalAppendingState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMergingState;
import org.apache.flink.shaded.guava18.com.google.common.collect.FluentIterable;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.api.windowing.assigners.BaseAlignedWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.MergingWindowSet;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;
import windowing.frames.FrameState;
import windowing.frames.FrameWindowing;
import windowing.windows.DataDrivenWindow;

import java.io.Serializable;
import java.util.*;
import java.util.function.*;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An operator that implements the logic for windowing based on a {@link WindowAssigner} and
 * {@link Trigger}.
 *
 * <p>When an element arrives it gets assigned a key using a {@link KeySelector} and it gets
 * assigned to zero or more windows using a {@link WindowAssigner}. Based on this, the element
 * is put into panes. A pane is the bucket of elements that have the same key and same
 * {@code Window}. An element can be in multiple panes if it was assigned to multiple windows by the
 * {@code WindowAssigner}.
 *
 * <p>Each pane gets its own instance of the provided {@code Trigger}. This singleBufferTrigger determines when
 * the contents of the pane should be processed to emit results. When a singleBufferTrigger fires,
 * the given {@link InternalWindowFunction} is invoked to produce the results that are emitted for
 * the pane to which the {@code Trigger} belongs.
 *
 * @param <K> The type of key returned by the {@code KeySelector}.
 * @param <IN> The type of the incoming elements.
 * @param <OUT> The type of elements emitted by the {@code InternalWindowFunction}.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
@Internal
public class StateAwareMultiBufferWindowOperator<K, IN, ACC, OUT, W extends Window>
        extends AbstractUdfStreamOperator<OUT, InternalWindowFunction<ACC, OUT, K, W>>
        implements OneInputStreamOperator<IN, OUT>, Triggerable<K, W> {

    private static final long serialVersionUID = 1L;

    // ------------------------------------------------------------------------
    // Configuration values and user functions
    // ------------------------------------------------------------------------
    private transient int stateSize;

    protected final WindowAssigner<? super IN, W> windowAssigner;

    private final KeySelector<IN, K> keySelector;

    protected final Trigger<? super IN, ? super W> trigger;

    private final StateDescriptor<? extends AppendingState<StreamRecord<IN>, Iterable<StreamRecord<IN>>>, ?> windowStateDescriptor;

    /**
     * For serializing the key in checkpoints.
     */
    protected final TypeSerializer<K> keySerializer;

    /**
     * For serializing the window in checkpoints.
     */
    protected final TypeSerializer<W> windowSerializer;

    /**
     * The allowed lateness for elements. This is used for:
     * <ul>
     *     <li>Deciding if an element should be dropped from a window due to lateness.
     *     <li>Clearing the state of a window if the system time passes the
     *         {@code window.maxTimestamp + allowedLateness} landmark.
     * </ul>
     */
    protected final long allowedLateness;

    /**
     * {@link OutputTag} to use for late arriving events. Elements for which
     * {@code window.maxTimestamp + allowedLateness} is smaller than the current watermark will
     * be emitted to this.
     */
    protected final OutputTag<IN> lateDataOutputTag;

    private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";

    protected transient Counter numLateRecordsDropped;

    // ------------------------------------------------------------------------
    // State that is not checkpointed
    // ------------------------------------------------------------------------

    /**
     * The state in which the window contents is stored. Each window is a namespace
     */
    private transient InternalAppendingState<K, W, StreamRecord<IN>, List<StreamRecord<IN>>, Iterable<StreamRecord<IN>>> windowState;

    /**
     * The {@link #windowState}, typed to merging state for merging windows.
     * Null if the window state is not mergeable.
     */
    private transient InternalSplitAndMergeState<K, W, StreamRecord<IN>, List<StreamRecord<IN>>, Iterable<StreamRecord<IN>>> windowMergingState;

    /**
     * The state that holds the merging window metadata (the sets that describe what is merged).
     */
    private transient InternalListState<K, VoidNamespace, Tuple2<W, W>> mergingSetsState;

    /**
     * This is given to the {@code InternalWindowFunction} for emitting elements with a given
     * timestamp.
     */
    protected transient TimestampedCollector<OUT> timestampedCollector;

    protected transient Context triggerContext = new Context(null, null);

    protected transient WindowContext processContext;

    protected transient BiFunction<Long, Long, W> windowFactory;

    protected transient BiPredicate<StreamRecord<IN>, W> windowMatcher;

    protected transient WindowAssigner.WindowAssignerContext windowAssignerContext;

    // ------------------------------------------------------------------------
    // State that needs to be checkpointed
    // ------------------------------------------------------------------------

    protected transient InternalTimerService<W> internalTimerService;

    final OutputTag<String> outputTag = new OutputTag<String>("latency") {
    };

    /**
     * Creates a new {@code WindowOperator} based on the given policies and user functions.
     */
    public StateAwareMultiBufferWindowOperator(
            WindowAssigner<? super IN, W> windowAssigner,
            TypeSerializer<W> windowSerializer,
            KeySelector<IN, K> keySelector,
            TypeSerializer<K> keySerializer,
            StateDescriptor<? extends AppendingState<StreamRecord<IN>, Iterable<StreamRecord<IN>>>, ?> windowStateDescriptor,
            InternalWindowFunction<ACC, OUT, K, W> windowFunction,
            Trigger<? super IN, ? super W> trigger,
            long allowedLateness,
            OutputTag<IN> lateDataOutputTag) {

        super(windowFunction);

        checkArgument(!(windowAssigner instanceof BaseAlignedWindowAssigner),
                "The " + windowAssigner.getClass().getSimpleName() + " cannot be used with a WindowOperator. " +
                        "This assigner is only used with the AccumulatingProcessingTimeWindowOperator and " +
                        "the AggregatingProcessingTimeWindowOperator");

        checkArgument(allowedLateness >= 0);

        checkArgument(windowStateDescriptor == null || windowStateDescriptor.isSerializerInitialized(),
                "window state serializer is not properly initialized");

        this.windowAssigner = checkNotNull(windowAssigner);
        this.windowSerializer = checkNotNull(windowSerializer);
        this.keySelector = checkNotNull(keySelector);
        this.keySerializer = checkNotNull(keySerializer);
        this.windowStateDescriptor = windowStateDescriptor;
        this.trigger = checkNotNull(trigger);
        this.allowedLateness = allowedLateness;
        this.lateDataOutputTag = lateDataOutputTag;

        setChainingStrategy(ChainingStrategy.ALWAYS);
    }

    @Override
    public void open() throws Exception {
        super.open();

        this.numLateRecordsDropped = metrics.counter(LATE_ELEMENTS_DROPPED_METRIC_NAME);
        timestampedCollector = new TimestampedCollector<>(output);

        internalTimerService =
                getInternalTimerService("window-timers", windowSerializer, this);

        triggerContext = new Context(null, null);
        processContext = new WindowContext(null);


        // create (or restore) the state that hold the actual window contents
        // NOTE - the state may be null in the case of the overriding evicting window operator
        if (windowStateDescriptor != null) {
            windowState = (InternalAppendingState<K, W, StreamRecord<IN>, List<StreamRecord<IN>>, Iterable<StreamRecord<IN>>>) getOrCreateKeyedState(windowSerializer, windowStateDescriptor);
        }

        this.windowAssignerContext = new StateAwareWindowAssigner.StateAwareWindowAssignerContext<IN, W>() {
            @Override
            public ValueState<FrameState> getCurrentFrameState(ValueStateDescriptor<FrameState> stateDescriptor) {
                try {
                    return StateAwareMultiBufferWindowOperator.this.getPartitionedState(stateDescriptor);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public MapState<Long, FrameState> getPastFrameState(MapStateDescriptor<Long, FrameState> stateDescriptor) {
                try {
                    return StateAwareMultiBufferWindowOperator.this.getPartitionedState(stateDescriptor);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public Iterable<StreamRecord<IN>> getContent(W window) {
                try {
                    W stateWindow = getSplittingMergingWindowSet().getStateWindow(window);
                    windowMergingState.setCurrentNamespace(stateWindow);
                    return StateAwareMultiBufferWindowOperator.this.windowMergingState.get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public long getCurrentWatermark() {
                return StateAwareMultiBufferWindowOperator.this.internalTimerService.currentWatermark();
            }

            @Override
            public long getAllowedLateness() {
                return StateAwareMultiBufferWindowOperator.this.allowedLateness;
            }

            @Override
            public long getCurrentProcessingTime() {
                return System.currentTimeMillis();
            }
        };


        if (windowAssigner instanceof StateAwareWindowAssigner) {
            this.windowFactory = (BiFunction<Long, Long, W>) ((StateAwareWindowAssigner<IN, W>) windowAssigner).getWindowFactory();
            this.windowMatcher = ((StateAwareWindowAssigner<IN, W>) windowAssigner).getWindowMatcher();
        }

        // store a typed reference for the state of merging windows - sanity check
        if (windowState instanceof InternalMergingState) {
            windowMergingState = new SplittingHeapListState<>((InternalMergingState<K, W, StreamRecord<IN>, List<StreamRecord<IN>>, Iterable<StreamRecord<IN>>>) windowState);
        }


        @SuppressWarnings("unchecked") final Class<Tuple2<W, W>> typedTuple = (Class<Tuple2<W, W>>) (Class<?>) Tuple2.class;

        final TupleSerializer<Tuple2<W, W>> tupleSerializer = new TupleSerializer<>(
                typedTuple,
                new TypeSerializer[]{windowSerializer, windowSerializer});

        final ListStateDescriptor<Tuple2<W, W>> mergingSetsStateDescriptor =
                new ListStateDescriptor<>("merging-window-set", tupleSerializer);

        // get the state that stores the merging sets
        mergingSetsState = (InternalListState<K, VoidNamespace, Tuple2<W, W>>)
                getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, mergingSetsStateDescriptor);
        mergingSetsState.setCurrentNamespace(VoidNamespace.INSTANCE);

    }

    @Override
    public void close() throws Exception {
        super.close();
        timestampedCollector = null;
        triggerContext = null;
        processContext = null;
        windowAssignerContext = null;
    }

    @Override
    public void dispose() throws Exception {
        super.dispose();
        timestampedCollector = null;
        triggerContext = null;
        processContext = null;
        windowAssignerContext = null;
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {

        //TODO: MB SCOPE Start
        //Ahmed Awad
        Long start = System.nanoTime();
//        System.out.print("Scope start "+ System.currentTimeMillis() + "-");
        final Collection<W> elementWindows = windowAssigner.assignWindows(
                element.getValue(), element.getTimestamp(), windowAssignerContext);


        //if element is handled by none of assigned elementWindows
        boolean isSkippedElement = true;
        boolean ooo = false;

        final K key = this.<K>getKeyedStateBackend().getCurrentKey();

        SplittingMergingWindowSet<W, Long> mergingWindows = getSplittingMergingWindowSet();


        //TODO state size naive impl

        stateSize = 0;
        mergingWindows.getKeys().forEach(w -> {
            windowState.setCurrentNamespace(w);
            Iterable<StreamRecord<IN>> streamRecords = null;
            try {
                streamRecords = windowState.get();
                streamRecords.forEach(r -> stateSize++);

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        if (windowAssigner instanceof MergingWindowAssigner) {

            Set<W> definitiveElementWindows = new HashSet<>();
            if (elementWindows.stream().anyMatch(w -> w.maxTimestamp() > element.getTimestamp())) {
                outOfOrderProcessing(element, key, false, elementWindows, mergingWindows, definitiveElementWindows);
                ooo = true;
            } else {
                definitiveElementWindows.addAll(elementWindows);
            }


            for (W window : definitiveElementWindows
                    .stream()
                    .sorted(Comparator.comparingLong(Window::maxTimestamp))
                    .collect(Collectors.toList())) {

                // adding the new window might result in a merge, in that case the actualWindow
                // is the merged window and we work with that. If we don't merge then
                // actualWindow == window
                //TODO: MB MERGE Start (1st Part)
                W actualWindow = mergingWindows.addWindow(window, new SplittingMergingWindowSet.MergeFunction<W>() {
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

                        for (W m: mergedWindows) {
                            triggerContext.window = m;
                            triggerContext.clear();
                            deleteCleanupTimer(m);
                        }

                        // merge the merged state windows into the newly resulting state window
                        windowMergingState.mergeNamespaces(stateWindowResult, mergedStateWindows);
                    }
                });
                //TODO: MB MERGE End (1st Part)
                //TODO: MB SCOPE End
                //Ahmed Awad
//                System.out.println("scope "+ (System.nanoTime() - start));
//                this.processContext.output(outputTag, "scope "+ (System.nanoTime() - start));
                this.output.collect(outputTag, new StreamRecord<String>("scope " + (System.nanoTime() - start)));
                // drop if the window is already late
                if (isWindowLate(actualWindow)) {
                    mergingWindows.retireWindow(actualWindow);
                    continue;
                }
                isSkippedElement = false;

                //TODO: MB ADD Start
                W stateWindow = mergingWindows.getStateWindow(actualWindow);
                if (stateWindow == null) {
                    throw new IllegalStateException("Window " + window + " is not in in-flight window set.");
                }


                windowState.setCurrentNamespace(stateWindow);

                //Added the precondition since, for frames, events that triggers a window may not belong to it
                if (windowMatcher.test(element, actualWindow) && !ooo) {
                    windowState.add(element);
                }
                //TODO: MB ADD End

                //TODO: MB REPORT Start
                triggerContext.key = key;
                triggerContext.window = actualWindow;

                TriggerResult triggerResult = triggerContext.onElement(element);
                //TODO: MB REPORT End

                if (triggerResult.isFire()) {
                    //TODO: MB CONTENT Start
                    ACC contents = (ACC) windowState.get();
                    if (contents == null) {
                        continue;
                    }
                    //TODO: MB CONTENT End
                    emitWindowContents(actualWindow, contents);
                }

                if (triggerResult.isPurge()) {
                    windowState.clear();
                }

                MapState<Long, FrameState> candidateWindowState = getPartitionedState(new MapStateDescriptor<>("candidateWindows", new LongSerializer(), new FrameState.Serializer()));

                //TODO: MB EVICT Start (1st Part)
                //PURGING
                if (candidateWindowState.get(((TimeWindow) actualWindow).getStart()).isClosed()) {
//                    candidateWindowState.remove(((TimeWindow)actualWindow).getStart());
//                    clearAllState(triggerContext.window, windowState, mergingWindows);
                    registerCleanupTimer(actualWindow);
                }
                //TODO: MB EVICT End (1st Part)
            }

            // need to make sure to update the merging state in state
            mergingWindows.persist();
        } else {
            for (W window : elementWindows) {

                // drop if the window is already late
                if (isWindowLate(window)) {
                    continue;
                }
                isSkippedElement = false;

                windowState.setCurrentNamespace(window);
                windowState.add(element);

                triggerContext.key = key;
                triggerContext.window = window;

                TriggerResult triggerResult = triggerContext.onElement(element);

                if (triggerResult.isFire()) {
                    ACC contents = (ACC) windowState.get();
                    if (contents == null) {
                        continue;
                    }
                    emitWindowContents(window, contents);
                }

                if (triggerResult.isPurge()) {
                    windowState.clear();
                }
                registerCleanupTimer(window);
            }
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

    private void outOfOrderProcessing(StreamRecord<IN> element, K key, boolean elementInserted, Collection<W> currentWindows,
                                      SplittingMergingWindowSet<W, Long> mergingWindows, Set<W> recomputedWindowsAccumulator) throws Exception {

        recomputedWindowsAccumulator.addAll(currentWindows);

        if (currentWindows.stream()
                .allMatch(w -> (w instanceof DataDrivenWindow))) {

            //TODO: SPLIT Start
            // First the window is splitted both in the StateWindow Backend and the MerginWindow Backend
            windowSplit(currentWindows, mergingWindows, false);
            //TODO: SPLIT End

            if (!elementInserted) {
                insertElement(element, currentWindows, mergingWindows);
                elementInserted = true;
            }

            //TODO: MERGE Start (2nd Part)
            // Then the merge operation is computed
            long recomputationTime = windowMerge(key, currentWindows, mergingWindows);
            //TODO: MERGE End (2nd Part)


            // followed by a recomputation in the scope, checking first that there exists a recomputation time (!=-1)
            Collection<W> recomputedWindows = Collections.emptyList();
            if (recomputationTime != -1)
                recomputedWindows = windowAssigner.assignWindows(null, recomputationTime, windowAssignerContext);

            Optional<W> maximumWindowOfCurrentIteration = currentWindows.stream()
                    .max(Comparator.comparingLong(Window::maxTimestamp));

            // NOTE: this should be always present
            if (maximumWindowOfCurrentIteration.isPresent()) {
                if (!recomputedWindows.stream().filter(w -> ((DataDrivenWindow) w).isClosed()).collect(Collectors.toSet()).contains(maximumWindowOfCurrentIteration.get())) {
                    outOfOrderProcessing(element, key, elementInserted, recomputedWindows, mergingWindows, recomputedWindowsAccumulator);
                } else {
                    //TODO: SPLIT Start
                    windowSplit(recomputedWindows, mergingWindows, true);
                    //TODO: SPLIT End
                }
            }
        }
    }

    private long windowMerge(K key, Collection<W> elementWindows, SplittingMergingWindowSet<W, Long> mergingWindows) {
        try {
            Optional<W> optionalStillOpenWindow = elementWindows.stream().filter(w -> !((DataDrivenWindow) w).isClosed()).findFirst();

            if (optionalStillOpenWindow.isPresent()) {
                W stillOpenWindow = optionalStillOpenWindow.get();
                ((DataDrivenWindow) stillOpenWindow).setRecomputing(true);
                mergingWindows.mergeRebalancing(stillOpenWindow, (mergeResult, mergedWindows, stateWindowResult, mergedStateWindows) -> {

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

                    if (windowAssigner instanceof FrameWindowing) {
                        ((FrameWindowing<? super IN>) windowAssigner).mergeFrames((DataDrivenWindow) mergeResult, windowAssignerContext);
                    }

                    // merge the merged state windows into the newly resulting state window
                    windowMergingState.mergeNamespaces(stateWindowResult, mergedStateWindows);
                });

                mergingWindows.persist();

                return ((DataDrivenWindow) stillOpenWindow).getStart();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }

    private void insertElement(StreamRecord<IN> element, Collection<W> elementWindows, SplittingMergingWindowSet<W, Long> mergingWindows) {
        elementWindows.stream()
                .filter(w -> windowMatcher.test(element, w))
                .forEach(w -> {
                    try {
                        mergingWindows.getStateWindow(w);
                        windowState.setCurrentNamespace(w);
                        windowState.add(element);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
    }

    private void windowSplit(Collection<W> elementWindows, SplittingMergingWindowSet<W, Long> mergingWindows, boolean leaveLastWindowClosed) throws Exception {
        Optional<Long> minimumStart = elementWindows.stream()
                .filter(w -> w instanceof TimeWindow)
                .map(w -> ((TimeWindow) w).getStart())
                .min(Long::compareTo);

        Optional<Long> maximumEnd = elementWindows.stream()
                .filter(w -> w instanceof TimeWindow)
                .map(w -> ((TimeWindow) w).getEnd())
                .max(Long::compareTo);

        if (minimumStart.isPresent() && maximumEnd.isPresent() &&
            windowMatcher != null && windowFactory != null) {
            W windowToSplit = windowFactory.apply(minimumStart.get(), maximumEnd.get());
            W originalWindowToSplit = mergingWindows.getStateWindow(windowToSplit);

            windowMergingState.splitNamespaces(originalWindowToSplit, elementWindows, windowMatcher);
            List<Pair<Long, W>> splittingPointsStateWindowsPairs = elementWindows.stream()
                    .map((Function<W, Pair<Long, W>>) w -> new ImmutablePair<>(w.maxTimestamp() + 1, w))
                    .collect(Collectors.toList());

            mergingWindows.splitMergedWindow(windowToSplit, splittingPointsStateWindowsPairs, (target, splittingPoint) -> {
                W leftMost = windowFactory.apply(((TimeWindow) target).getStart(), splittingPoint);
                W rightMost = windowFactory.apply(splittingPoint, target.maxTimestamp() + 1);
                return new ImmutablePair<>(leftMost, rightMost);
            }, leaveLastWindowClosed);
            mergingWindows.persist();
        }
    }

    @Override
    public void onEventTime(InternalTimer<K, W> timer) throws Exception {
        triggerContext.key = timer.getKey();
        triggerContext.window = timer.getNamespace();

        SplittingMergingWindowSet<W, Long> mergingWindows;

        if (windowAssigner instanceof MergingWindowAssigner) {
            mergingWindows = getSplittingMergingWindowSet();
            W stateWindow = mergingWindows.getStateWindow(triggerContext.window);
            if (stateWindow == null) {
                // Timer firing for non-existent window, this can only happen if a
                // singleBufferTrigger did not clean up timers. We have already cleared the merging
                // window and therefore the Trigger state, however, so nothing to do.
                return;
            } else {
                windowState.setCurrentNamespace(stateWindow);
            }
        } else {
            windowState.setCurrentNamespace(triggerContext.window);
            mergingWindows = null;
        }

        TriggerResult triggerResult = triggerContext.onEventTime(timer.getTimestamp());

        if (triggerResult.isFire()) {
            ACC contents = (ACC) windowState.get();
            if (contents != null) {
                emitWindowContents(triggerContext.window, contents);
            }
        }

        if (triggerResult.isPurge()) {
            windowState.clear();
        }

        //TODO: EVICT Start (2nd Part)
        if (windowAssigner.isEventTime() && isCleanupTime(triggerContext.window, timer.getTimestamp())) {
            clearAllState(triggerContext.window, (AppendingState<IN, ACC>) windowState, mergingWindows);
        }

        if (mergingWindows != null) {
            // need to make sure to update the merging state in state
            mergingWindows.persist();
        }
        //TODO: EVICT End (2nd Part)
    }

    @Override
    public void onProcessingTime(InternalTimer<K, W> timer) throws Exception {
        triggerContext.key = timer.getKey();
        triggerContext.window = timer.getNamespace();

        SplittingMergingWindowSet<W, Long> mergingWindows;

        if (windowAssigner instanceof MergingWindowAssigner) {
            mergingWindows = getSplittingMergingWindowSet();
            W stateWindow = mergingWindows.getStateWindow(triggerContext.window);
            if (stateWindow == null) {
                // Timer firing for non-existent window, this can only happen if a
                // singleBufferTrigger did not clean up timers. We have already cleared the merging
                // window and therefore the Trigger state, however, so nothing to do.
                return;
            } else {
                windowState.setCurrentNamespace(stateWindow);
            }
        } else {
            windowState.setCurrentNamespace(triggerContext.window);
            mergingWindows = null;
        }

        TriggerResult triggerResult = triggerContext.onProcessingTime(timer.getTimestamp());

        if (triggerResult.isFire()) {
            ACC contents = (ACC) windowState.get();
            if (contents != null) {
                emitWindowContents(triggerContext.window, contents);
            }
        }

        if (triggerResult.isPurge()) {
            windowState.clear();
        }

        if (!windowAssigner.isEventTime() && isCleanupTime(triggerContext.window, timer.getTimestamp())) {
            clearAllState(triggerContext.window, (AppendingState<IN, ACC>) windowState, mergingWindows);
        }

        if (mergingWindows != null) {
            // need to make sure to update the merging state in state
            mergingWindows.persist();
        }
    }

    /**
     * Drops all state for the given window and calls
     * {@link Trigger#clear(Window, Trigger.TriggerContext)}.
     *
     * <p>The caller must ensure that the
     * correct key is set in the state backend and the triggerContext object.
     */
    private void clearAllState(
            W window,
            AppendingState<IN, ACC> windowState,
            SplittingMergingWindowSet<W, Long> mergingWindows) throws Exception {
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
     * Emits the contents of the given window using the {@link InternalWindowFunction}.
     */
    @SuppressWarnings("unchecked")
    private void emitWindowContents(W window, ACC contents) throws Exception {

        FluentIterable<IN> projectedContents = FluentIterable.from((Iterable<StreamRecord<IN>>) contents)
                .transform(input -> {
                    if (input.getValue() == null)
                        System.out.println("cjsddnasd");
                    return input.getValue();
                });
        timestampedCollector.setAbsoluteTimestamp(window.maxTimestamp());
        processContext.window = window;
        userFunction.process(triggerContext.key, window, processContext, (ACC) projectedContents, timestampedCollector);
    }

    /**
     * Write skipped late arriving element to SideOutput.
     *
     * @param element skipped late arriving element to side output
     */
    protected void sideOutput(StreamRecord<IN> element) {
        output.collect(lateDataOutputTag, element);
    }

    /**
     * Retrieves the {@link MergingWindowSet} for the currently active key.
     * The caller must ensure that the correct key is set in the state backend.
     *
     * <p>The caller must also ensure to properly persist changes to state using
     * {@link MergingWindowSet#persist()}.
     */
    protected SplittingMergingWindowSet<W, Long> getSplittingMergingWindowSet() throws Exception {
        @SuppressWarnings("unchecked")
        MergingWindowAssigner<? super IN, W> mergingAssigner = (MergingWindowAssigner<? super IN, W>) windowAssigner;
        return new SplittingMergingWindowSet<>(mergingAssigner, mergingSetsState);
    }

    protected MergingWindowSet<W> getMergingWindowSet() throws Exception {
        @SuppressWarnings("unchecked")
        MergingWindowAssigner<? super IN, W> mergingAssigner = (MergingWindowAssigner<? super IN, W>) windowAssigner;
        return new MergingWindowSet<>(mergingAssigner, mergingSetsState);
    }

    /**
     * Returns {@code true} if the watermark is after the end timestamp plus the allowed lateness
     * of the given window.
     */
    protected boolean isWindowLate(W window) {
        return (windowAssigner.isEventTime() && (cleanupTime(window) <= internalTimerService.currentWatermark()));
    }

    /**
     * Decide if a record is currently late, based on current watermark and allowed lateness.
     *
     * @param element The element to check
     * @return The element for which should be considered when sideoutputs
     */
    protected boolean isElementLate(StreamRecord<IN> element) {
        return (windowAssigner.isEventTime()) &&
               (element.getTimestamp() + allowedLateness <= internalTimerService.currentWatermark());
    }

    /**
     * Registers a timer to cleanup the content of the window.
     *
     * @param window the window whose state to discard
     */
    protected void registerCleanupTimer(W window) {
        long cleanupTime = cleanupTime(window);
        if (cleanupTime == Long.MAX_VALUE) {
            // don't set a GC timer for "end of time"
            return;
        }

        if (windowAssigner.isEventTime()) {
            triggerContext.registerEventTimeTimer(cleanupTime);
        } else {
            triggerContext.registerProcessingTimeTimer(cleanupTime);
        }
    }

    /**
     * Deletes the cleanup timer set for the contents of the provided window.
     *
     * @param window the window whose state to discard
     */
    protected void deleteCleanupTimer(W window) {
        long cleanupTime = cleanupTime(window);
        if (cleanupTime == Long.MAX_VALUE) {
            // no need to clean up because we didn't set one
            return;
        }
        if (windowAssigner.isEventTime()) {
            triggerContext.deleteEventTimeTimer(cleanupTime);
        } else {
            triggerContext.deleteProcessingTimeTimer(cleanupTime);
        }
    }

    /**
     * Returns the cleanup time for a window, which is
     * {@code window.maxTimestamp + allowedLateness}. In
     * case this leads to a value greater than {@link Long#MAX_VALUE}
     * then a cleanup time of {@link Long#MAX_VALUE} is
     * returned.
     *
     * @param window the window whose cleanup time we are computing.
     */
    private long cleanupTime(W window) {
        if (windowAssigner.isEventTime()) {
            long cleanupTime = window.maxTimestamp() + allowedLateness;
            return cleanupTime >= window.maxTimestamp() ? cleanupTime : Long.MAX_VALUE;
        } else {
            return window.maxTimestamp();
        }
    }

    /**
     * Returns {@code true} if the given time is the cleanup time for the given window.
     */
    protected final boolean isCleanupTime(W window, long time) {
        return time == cleanupTime(window);
    }

    /**
     * Base class for per-window {@link KeyedStateStore KeyedStateStores}. Used to allow per-window
     * state access for {@link org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction}.
     */
    public abstract class AbstractPerWindowStateStore extends DefaultKeyedStateStore {

        // we have this in the base class even though it's not used in MergingKeyStore so that
        // we can always set it and ignore what actual implementation we have
        protected W window;

        public AbstractPerWindowStateStore(KeyedStateBackend<?> keyedStateBackend, ExecutionConfig executionConfig) {
            super(keyedStateBackend, executionConfig);
        }
    }

    /**
     * Special {@link AbstractPerWindowStateStore} that doesn't allow access to per-window state.
     */
    public class MergingWindowStateStore extends AbstractPerWindowStateStore {

        public MergingWindowStateStore(KeyedStateBackend<?> keyedStateBackend, ExecutionConfig executionConfig) {
            super(keyedStateBackend, executionConfig);
        }

        @Override
        public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
            throw new UnsupportedOperationException("Per-window state is not allowed when using merging windows.");
        }

        @Override
        public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
            throw new UnsupportedOperationException("Per-window state is not allowed when using merging windows.");
        }

        @Override
        public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
            throw new UnsupportedOperationException("Per-window state is not allowed when using merging windows.");
        }

        @Override
        public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) {
            throw new UnsupportedOperationException("Per-window state is not allowed when using merging windows.");
        }

        @Override
        public <T, A> FoldingState<T, A> getFoldingState(FoldingStateDescriptor<T, A> stateProperties) {
            throw new UnsupportedOperationException("Per-window state is not allowed when using merging windows.");
        }

        @Override
        public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
            throw new UnsupportedOperationException("Per-window state is not allowed when using merging windows.");
        }
    }

    /**
     * Regular per-window state store for use with
     * {@link org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction}.
     */
    public class PerWindowStateStore extends AbstractPerWindowStateStore {

        public PerWindowStateStore(KeyedStateBackend<?> keyedStateBackend, ExecutionConfig executionConfig) {
            super(keyedStateBackend, executionConfig);
        }

        @Override
        protected <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) throws Exception {
            return keyedStateBackend.getPartitionedState(
                    window,
                    windowSerializer,
                    stateDescriptor);
        }
    }

    /**
     * A utility class for handling {@code ProcessWindowFunction} invocations. This can be reused
     * by setting the {@code key} and {@code window} fields. No internal state must be kept in the
     * {@code WindowContext}.
     */
    public class WindowContext implements InternalWindowFunction.InternalWindowContext {
        protected W window;

        protected AbstractPerWindowStateStore windowState;

        public WindowContext(W window) {
            this.window = window;
            this.windowState = windowAssigner instanceof MergingWindowAssigner ?
                    new MergingWindowStateStore(getKeyedStateBackend(), getExecutionConfig()) :
                    new PerWindowStateStore(getKeyedStateBackend(), getExecutionConfig());
        }

        @Override
        public String toString() {
            return "WindowContext{Window = " + window.toString() + "}";
        }

        public void clear() throws Exception {
            userFunction.clear(window, this);
        }

        @Override
        public long currentProcessingTime() {
            return internalTimerService.currentProcessingTime();
        }

        @Override
        public long currentWatermark() {
            return internalTimerService.currentWatermark();
        }

        @Override
        public KeyedStateStore windowState() {
            this.windowState.window = this.window;
            return this.windowState;
        }

        @Override
        public KeyedStateStore globalState() {
            return StateAwareMultiBufferWindowOperator.this.getKeyedStateStore();
        }

        @Override
        public <X> void output(OutputTag<X> outputTag, X value) {
            if (outputTag == null) {
                throw new IllegalArgumentException("OutputTag must not be null.");
            }
            output.collect(outputTag, new StreamRecord<>(value, window.maxTimestamp()));
        }
    }

    /**
     * {@code Context} is a utility for handling {@code Trigger} invocations. It can be reused
     * by setting the {@code key} and {@code window} fields. No internal state must be kept in
     * the {@code Context}
     */
    public class Context implements Trigger.OnMergeContext {
        protected K key;
        protected W window;

        protected Collection<W> mergedWindows;

        public Context(K key, W window) {
            this.key = key;
            this.window = window;
        }

        @Override
        public MetricGroup getMetricGroup() {
            return StateAwareMultiBufferWindowOperator.this.getMetricGroup();
        }

        public long getCurrentWatermark() {
            return internalTimerService.currentWatermark();
        }

        @Override
        public <S extends Serializable> ValueState<S> getKeyValueState(String name,
                                                                       Class<S> stateType,
                                                                       S defaultState) {
            checkNotNull(stateType, "The state type class must not be null");

            TypeInformation<S> typeInfo;
            try {
                typeInfo = TypeExtractor.getForClass(stateType);
            } catch (Exception e) {
                throw new RuntimeException("Cannot analyze type '" + stateType.getName() +
                                           "' from the class alone, due to generic type parameters. " +
                                           "Please specify the TypeInformation directly.", e);
            }

            return getKeyValueState(name, typeInfo, defaultState);
        }

        @Override
        public <S extends Serializable> ValueState<S> getKeyValueState(String name,
                                                                       TypeInformation<S> stateType,
                                                                       S defaultState) {

            checkNotNull(name, "The name of the state must not be null");
            checkNotNull(stateType, "The state type information must not be null");

            ValueStateDescriptor<S> stateDesc = new ValueStateDescriptor<>(name, stateType.createSerializer(getExecutionConfig()), defaultState);
            return getPartitionedState(stateDesc);
        }

        @SuppressWarnings("unchecked")
        public <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) {
            try {
                return StateAwareMultiBufferWindowOperator.this.getPartitionedState(window, windowSerializer, stateDescriptor);
            } catch (Exception e) {
                throw new RuntimeException("Could not retrieve state", e);
            }
        }

        @Override
        public <S extends MergingState<?, ?>> void mergePartitionedState(StateDescriptor<S, ?> stateDescriptor) {
            if (mergedWindows != null && mergedWindows.size() > 0) {
                try {
                    S rawState = getKeyedStateBackend().getOrCreateKeyedState(windowSerializer, stateDescriptor);

                    if (rawState instanceof InternalMergingState) {
                        @SuppressWarnings("unchecked")
                        InternalMergingState<K, W, ?, ?, ?> mergingState = (InternalMergingState<K, W, ?, ?, ?>) rawState;
                        mergingState.mergeNamespaces(window, mergedWindows);
                    } else {
                        throw new IllegalArgumentException(
                                "The given state descriptor does not refer to a mergeable state (MergingState)");
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Error while merging state.", e);
                }
            }
        }

        @Override
        public long getCurrentProcessingTime() {
            return internalTimerService.currentProcessingTime();
        }

        @Override
        public void registerProcessingTimeTimer(long time) {
            internalTimerService.registerProcessingTimeTimer(window, time);
        }

        @Override
        public void registerEventTimeTimer(long time) {
            internalTimerService.registerEventTimeTimer(window, time);
        }

        @Override
        public void deleteProcessingTimeTimer(long time) {
            internalTimerService.deleteProcessingTimeTimer(window, time);
        }

        @Override
        public void deleteEventTimeTimer(long time) {
            internalTimerService.deleteEventTimeTimer(window, time);
        }

        public TriggerResult onElement(StreamRecord<IN> element) throws Exception {
            return trigger.onElement(element.getValue(), element.getTimestamp(), window, this);
        }

        public TriggerResult onProcessingTime(long time) throws Exception {
            return trigger.onProcessingTime(time, window, this);
        }

        public TriggerResult onEventTime(long time) throws Exception {
            return trigger.onEventTime(time, window, this);
        }

        public void onMerge(Collection<W> mergedWindows) throws Exception {
            this.mergedWindows = mergedWindows;
            trigger.onMerge(window, this);
        }

        public void clear() throws Exception {
            trigger.clear(window, this);
        }

        @Override
        public String toString() {
            return "Context{" +
                   "key=" + key +
                   ", window=" + window +
                   '}';
        }
    }

    /**
     * Internal class for keeping track of in-flight timers.
     */
    protected static class Timer<K, W extends Window> implements Comparable<Timer<K, W>> {
        protected long timestamp;
        protected K key;
        protected W window;

        public Timer(long timestamp, K key, W window) {
            this.timestamp = timestamp;
            this.key = key;
            this.window = window;
        }

        @Override
        public int compareTo(Timer<K, W> o) {
            return Long.compare(this.timestamp, o.timestamp);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()){
                return false;
            }

            Timer<?, ?> timer = (Timer<?, ?>) o;

            return timestamp == timer.timestamp
                    && key.equals(timer.key)
                    && window.equals(timer.window);

        }

        @Override
        public int hashCode() {
            int result = (int) (timestamp ^ (timestamp >>> 32));
            result = 31 * result + key.hashCode();
            result = 31 * result + window.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "Timer{" +
                   "timestamp=" + timestamp +
                   ", key=" + key +
                   ", window=" + window +
                   '}';
        }
    }

    // ------------------------------------------------------------------------
    // Getters for testing
    // ------------------------------------------------------------------------

    @VisibleForTesting
    public Trigger<? super IN, ? super W> getTrigger() {
        return trigger;
    }

    @VisibleForTesting
    public KeySelector<IN, K> getKeySelector() {
        return keySelector;
    }

    @VisibleForTesting
    public WindowAssigner<? super IN, W> getWindowAssigner() {
        return windowAssigner;
    }

    @VisibleForTesting
    public StateDescriptor<? extends AppendingState<StreamRecord<IN>, Iterable<StreamRecord<IN>>>, ?> getStateDescriptor() {
        return windowStateDescriptor;
    }
}
