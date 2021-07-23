/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package windowing.frames;



import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import windowing.SingleBufferWindowing;
import windowing.StateAwareWindowAssigner;
import windowing.windows.CandidateTimeWindow;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

public abstract class FrameWindowing<I> extends StateAwareWindowAssigner<I, TimeWindow> implements SingleBufferWindowing<I, GlobalWindow> {

    protected ValueStateDescriptor<FrameState> frameStateDescriptor = new ValueStateDescriptor<>("currentFrameState", new FrameState.Serializer());
    protected MapStateDescriptor<Long,FrameState> candidateWindowsDescriptor =
            new MapStateDescriptor<>("candidateWindows", new LongSerializer(), new FrameState.Serializer());
    protected ToLongFunction<I> toLongFunctionValue;
    protected Trigger.TriggerContext context;


    public FrameWindowing(ToLongFunction<I> toLongFunctionValue) {
        this.toLongFunctionValue = toLongFunctionValue;
    }

    @Override
    public Collection<TimeWindow> assignWindows(I element, long timestamp, WindowAssignerContext context) {


        // TODO: Add an eviction policy for the windows
        MapState<Long, FrameState> candidateWindowsState = ((StateAwareWindowAssignerContext)context).getPastFrameState(candidateWindowsDescriptor);
        ValueState<FrameState> currentFrameState = ((StateAwareWindowAssignerContext) context).getCurrentFrameState(frameStateDescriptor);

        long elementLong = toLongFunctionValue.applyAsLong(element);

        Iterator<CandidateTimeWindow> candidateTimeWindowIterator = processFrames(timestamp, candidateWindowsState, elementLong, currentFrameState);

        List<TimeWindow> finalWindows = new ArrayList<>();

        while(candidateTimeWindowIterator.hasNext()){
            CandidateTimeWindow candidateTimeWindow = candidateTimeWindowIterator.next();
            finalWindows.add(candidateTimeWindow.getFinalWindow());
        }

        return finalWindows;

    }

    protected Iterator<CandidateTimeWindow> processFrames(long timestamp, MapState<Long, FrameState> candidateWindowsState, long elementLong, ValueState<FrameState> currentFrameState){
        try {
            FrameState frameState = currentFrameState.value();

            if(frameState==null)
                frameState = FrameState.initializeFrameState(timestamp-2);

            if(timestamp >= frameState.getTsStart()){
                // Processing on the current Frame
                Collection<FrameState> resultingFrameStates = processFrame(timestamp, elementLong, frameState);
                currentFrameState.update(frameState);
                return getCandidateTimeWindowIterator(candidateWindowsState, resultingFrameStates);
            } else {
                // Out-Of-Order Processing on a Previous Frame


                for (Iterator<Long> it = candidateWindowsState.keys().iterator(); it.hasNext();){
                    Long windowStart = it.next();
                    // Locate the right Frame instance
                    if(timestamp>=windowStart && timestamp<candidateWindowsState.get(windowStart).getTsEnd() && candidateWindowsState.get(windowStart).isClosed()){
                        /*
                        If the size of the collection is equal to one, only the located frameState has been modified, i.e., no side effects
                        Else, a recomputation of the frame has been done, i.e., a split
                        All the splitted frame instances are valid, except the last one,
                        which may be merged with the next frame instances. For this reason, we sort them,
                        and return all of them, marking the last one for reprocessing (already marked by the fact that the window is not closed).
                         */

                        FrameState frameState1 = candidateWindowsState.get(windowStart);
                        Collection<FrameState> resultingFrameStates = processOutOfOrder(timestamp, elementLong, frameState1);
                        return getCandidateTimeWindowIterator(candidateWindowsState, resultingFrameStates);
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } return Collections.emptyIterator();
    }

    private Iterator<CandidateTimeWindow> getCandidateTimeWindowIterator(MapState<Long, FrameState> candidateWindowsState, Collection<FrameState> resultingFrameStates) {
        return resultingFrameStates.stream().map(frameState1 -> {
            try {
                candidateWindowsState.put(frameState1.getTsStart(), frameState1);
                return new CandidateTimeWindow(frameState1.getTsStart(), frameState1.getTsEnd(), frameState1.isClosed());
            } catch (Exception e) {
                e.printStackTrace();
            } return null;
        }).collect(Collectors.toList()).iterator();
    }

    /**
     *
     * @param timestamp
     * @param elementLong
     * @param frameState
     * @return Whether successive frames require recomputation
     */
    protected Collection<FrameState> processFrame(long timestamp, long elementLong, FrameState frameState){
        try {
            if(timestamp > frameState.getTsEnd()) {
                // Normal Processing
                Collection<FrameState> frameStateCollection = new LinkedList<>();
                if (closePred(elementLong, frameState))
                    // I added a collection to gather the closed window, as the frameState variable is overridden
                    close(timestamp, frameState, frameStateCollection);
                if (updatePred(elementLong, frameState)){
                    update(timestamp, elementLong, frameState);
                    frameStateCollection.add(frameState);
                }
                if (openPred(elementLong, frameState)){
                    open(timestamp, elementLong, frameState);
                    frameStateCollection.add(frameState);
                }
                return frameStateCollection;
            } else {
                //Out-Of-Order Processing on the frame
                return processOutOfOrder(timestamp, elementLong, frameState);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } return Collections.emptyList();
    }

    protected abstract boolean openPred(long element, FrameState mapState) throws Exception;

    protected abstract boolean updatePred(long element, FrameState mapState) throws Exception;

    protected abstract boolean closePred(long element, FrameState mapState) throws Exception;

    protected abstract void close(long ts, FrameState mapState, Collection<FrameState> resultingFrameState) throws Exception;

    protected abstract void open(long ts, long arg, FrameState mapState) throws Exception;

    protected abstract void update(long ts, long arg, FrameState windowAssignerContext) throws Exception;

    protected abstract Collection<FrameState> processOutOfOrder(long ts, long arg, FrameState rebuildingFrameState) throws Exception;


    @Override
    public BiFunction<Long, Long, TimeWindow> getWindowFactory() {
        return new BiFunction<Long, Long, TimeWindow>() {
            @Override
            public TimeWindow apply(Long aLong, Long aLong2) {
                return new TimeWindow(aLong, aLong2);
            }
        };
    }

    @Override
    public BiPredicate<StreamRecord<I>, TimeWindow> getWindowMatcher() {
        return (iStreamRecord, timeWindow) -> iStreamRecord.getTimestamp() >= timeWindow.getStart() && iStreamRecord.getTimestamp() < timeWindow.getEnd();
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public void mergeWindows(Collection<TimeWindow> windows, MergeCallback<TimeWindow> callback) {
        // sort the windows by the start time and then merge overlapping windows

        List<TimeWindow> sortedWindows = new ArrayList<>(windows);

        Collections.sort(sortedWindows, (o1, o2) -> Long.compare(o1.getStart(), o2.getStart()));

        List<Tuple2<TimeWindow, Set<TimeWindow>>> merged = new ArrayList<>();
        Tuple2<TimeWindow, Set<TimeWindow>> currentMerge = null;

        for (TimeWindow candidate: sortedWindows) {
            if (currentMerge == null) {
                currentMerge = new Tuple2<>();
                currentMerge.f0 = candidate;
                currentMerge.f1 = new HashSet<>();
                currentMerge.f1.add(candidate);
            } else if (intersects(currentMerge.f0, candidate)) {
                currentMerge.f0 = currentMerge.f0.cover(candidate);
                currentMerge.f1.add(candidate);
            } else {
                merged.add(currentMerge);
                currentMerge = new Tuple2<>();
                currentMerge.f0 = candidate;
                currentMerge.f1 = new HashSet<>();
                currentMerge.f1.add(candidate);
            }
        }

        if (currentMerge != null) {
            merged.add(currentMerge);
        }

        for (Tuple2<TimeWindow, Set<TimeWindow>> m: merged) {
            if (m.f1.size() > 1) {
                callback.merge(m.f1, m.f0);
            }
        }
    }

    private boolean intersects(TimeWindow w1, TimeWindow w2){
        return w1.getStart() <= w2.maxTimestamp() && w1.maxTimestamp() >= w2.getStart();
    }

    @Override
    public boolean isEventTime() {
        return true;
    }

    @Override
    public abstract Evictor<I, GlobalWindow> evictor();

    @Override
    public Trigger<I, GlobalWindow> singleBufferTrigger(){

        return new FrameTrigger();

    }

    @SuppressWarnings("unchecked")
    @Override
    public Trigger<I, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        return new Trigger<I, TimeWindow>() {
            @Override
            public TriggerResult onElement(I element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                return TriggerResult.FIRE;
            }

            @Override
            public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                return TriggerResult.CONTINUE;
            }

            @Override
            public boolean canMerge() {
                return true;
            }

            @Override
            public void onMerge(TimeWindow window, OnMergeContext ctx) throws Exception {

            }

            @Override
            public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                return TriggerResult.CONTINUE;
            }

            @Override
            public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            }
        };
    }

    protected class FrameTrigger extends Trigger<I, GlobalWindow> {

        //TODO: Add nested trigger for SECRET reporting variations

        public FrameTrigger() {
        }

        @Override
        public TriggerResult onElement(
                I element,
                long timestamp,
                GlobalWindow window,
                TriggerContext ctx) throws Exception {


            MapState<Long,FrameState> windowState = ctx.getPartitionedState(candidateWindowsDescriptor);
            ValueState<FrameState> currentFrameState = ((StateAwareWindowAssignerContext) context).getCurrentFrameState(frameStateDescriptor);

            long elementLong = toLongFunctionValue.applyAsLong(element);

            Iterator<CandidateTimeWindow> finalWindows = processFrames(timestamp, windowState, elementLong, currentFrameState);

            //Pre-filter: in case no windows is available, optimization, without going to the evictor
            if(finalWindows.hasNext()){
                context = ctx;
                return TriggerResult.FIRE;
            }
            else
                return TriggerResult.PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(
                long time,
                GlobalWindow window,
                TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(
                long time,
                GlobalWindow window,
                TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {
            ctx.getPartitionedState(candidateWindowsDescriptor).clear();
        }
    }

}