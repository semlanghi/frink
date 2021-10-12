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

import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.awt.*;
import java.util.*;
import java.util.function.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class DeltaWindowing<I> extends FrameWindowing<I> {

    private final long threshold;

    public DeltaWindowing(long threshold, ToLongFunction<I> toLongFunctionValue) {
        super(toLongFunctionValue);
        this.threshold = threshold;
    }

    @Override
    protected boolean openPred(long element, FrameState mapState) throws Exception {
        return mapState.getTsStart()==-1L ;
    }

    @Override
    protected boolean updatePred(long element, FrameState mapState) throws Exception {
        if(mapState.getAuxiliaryValue()==0)
            return false;
        return Math.abs(mapState.getAuxiliaryValue()-element) < threshold;
    }

    @Override
    protected boolean closePred(long element, FrameState mapState) throws Exception {
        if(mapState.getAuxiliaryValue()==0)
            return false;
        return Math.abs(mapState.getAuxiliaryValue()-element) >= threshold;
    }

    @Override
    protected void close(long ts, FrameState mapState, Collection<FrameState> resultingFrameState) throws Exception {
        mapState.extend(ts);
        mapState.close();
        resultingFrameState.add(mapState.copy());

        mapState.resetFrameState();
    }

    @Override
    protected void open(long ts, long arg, FrameState mapState) throws Exception {
        mapState.setTsStart(ts);
        mapState.setAuxiliaryValue(arg);
    }

    @Override
    protected void update(long ts, long arg, FrameState mapState) throws Exception {
        mapState.extend(ts+1);
    }

    @Override
    protected Collection<FrameState> processOutOfOrder(long ts, long arg, FrameState rebuildingFrameState, Iterable<StreamRecord<I>> iterable) throws Exception {
        // Reiterate on the elements arrived before arg
        FrameState frameStateFinal = StreamSupport.stream(iterable.spliterator(),false)
                .filter(iStreamRecord -> iStreamRecord.getTimestamp()<=ts)
                .reduce(FrameState.initializeFrameState(-1L),
                        (frameState, iStreamRecord) -> {
                            Collection<FrameState> frameStates = processFrame(iStreamRecord.getTimestamp(), toLongFunctionValue.applyAsLong(iStreamRecord.getValue()), frameState);
                            if (frameStates.size() == 1)
                                return frameStates.iterator().next();
                            return null;
                        }, (frameState, frameState2) -> {
                            if (frameState.getTsStart()==-1)
                                return frameState2;
                            if (frameState2.getTsStart()==-1)
                                return frameState;
                            return new FrameState(frameState.getCount()+ frameState2.getCount(),
                                    Math.min(frameState.getTsStart(), frameState2.getTsStart()),
                                    frameState.getTsStart() < frameState2.getTsStart() ? frameState.getAuxiliaryValue() : frameState2.getAuxiliaryValue(), 0L
                                    , Math.max(frameState.getTsEnd(), frameState2.getTsEnd()), frameState.isClosed() || frameState2.isClosed());
                        });

        //Process arg, whether it returns a splitted result (size>1) or a single, go to the frame not yet closed
        Collection<FrameState> withRecordFrameStates = null;
        //This is done only if we are not recomputing
        if(arg!=-1)
            withRecordFrameStates = processFrame(ts, arg, frameStateFinal);
        else {
            withRecordFrameStates = new ArrayList<>();
            withRecordFrameStates.add(frameStateFinal);
        }
        Optional<FrameState> optionalNotYetClosedFrameState = withRecordFrameStates.stream().filter(frameState -> !frameState.isClosed()).findFirst();

        if(optionalNotYetClosedFrameState.isPresent()){
            // Cannot use the previous stream, this is not a reduce operation as we may end up with multiple splitted frameStates
            FrameState notYetClosedState = optionalNotYetClosedFrameState.get();

            // Filter events arrived after arg
            Iterator<StreamRecord<I>> valuesIterator = iterable == null ? Collections.emptyIterator() : StreamSupport.stream(iterable.spliterator(),false)
                    .filter(iStreamRecord -> iStreamRecord.getTimestamp()>ts)
                    .sorted(Comparator.comparingLong(StreamRecord::getTimestamp))
                    .iterator();

            // Process these events, adding new closed frames everytime there is a split
            while (valuesIterator.hasNext()){
                StreamRecord<I> streamRecord = valuesIterator.next();
                Collection<FrameState> frameStates = processFrame(streamRecord.getTimestamp(), toLongFunctionValue.applyAsLong(streamRecord.getValue()), notYetClosedState);
                withRecordFrameStates.addAll(frameStates.stream().filter(FrameState::isClosed).collect(Collectors.toList()));
                Optional<FrameState> optionalFrameState = frameStates.stream().filter(frameState -> !frameState.isClosed()).findFirst();
                if(optionalFrameState.isPresent())
                    notYetClosedState = optionalFrameState.get();
                else{
                    Optional<FrameState> lastSplittedWindow = withRecordFrameStates.stream().max(Comparator.comparingLong(FrameState::getTsEnd));
                    lastSplittedWindow.ifPresent(frameState -> frameState.extend(rebuildingFrameState.getTsEnd()));
                    Optional<FrameState> firstSplittedWindow = withRecordFrameStates.stream().min(Comparator.comparingLong(FrameState::getTsStart));
                    firstSplittedWindow.ifPresent(frameState -> frameState.setTsStart(rebuildingFrameState.getTsStart()));
                    return withRecordFrameStates;
                }
            }
        }
        Optional<FrameState> lastSplittedWindow = withRecordFrameStates.stream().max(Comparator.comparingLong(FrameState::getTsEnd));
        lastSplittedWindow.ifPresent(frameState -> frameState.extend(rebuildingFrameState.getTsEnd()));
        Optional<FrameState> firstSplittedWindow = withRecordFrameStates.stream().min(Comparator.comparingLong(FrameState::getTsStart));
        firstSplittedWindow.ifPresent(frameState -> frameState.setStartPossibleInconsistent(rebuildingFrameState.getTsStart()));
        return withRecordFrameStates;
    }


    @Override
    public Evictor<I, GlobalWindow> singleBufferEvictor() {
        return new Evictor<I,GlobalWindow>() {
            @Override
            public void evictBefore(
                    Iterable<TimestampedValue<I>> elements,
                    int size,
                    GlobalWindow window,
                    EvictorContext evictorContext) {
            }

            @Override
            public void evictAfter(
                    Iterable<TimestampedValue<I>> elements,
                    int size,
                    GlobalWindow window,
                    EvictorContext evictorContext) {
                try {
                    for (Iterator<TimestampedValue<I>> itTsValues = elements.iterator(); itTsValues.hasNext();){
                        itTsValues.next();
                        itTsValues.remove();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
    }




}
