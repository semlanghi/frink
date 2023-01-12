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
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.*;
import java.util.function.ToLongFunction;

//TODO: Fix problem in the evaluation
public class ThresholdWindowing<I> extends FrameWindowing<I>{

    private final int min = 0;
    private final long threshold;


    public ThresholdWindowing(long threshold, ToLongFunction<I> toLongFunctionValue) {
        super(toLongFunctionValue);
        this.threshold = threshold;
    }

    @Override
    protected boolean openPred(long element, FrameState mapState) throws Exception {
        return threshold <= element &&
                mapState.getCount()==0L;
    }

    @Override
    protected boolean updatePred(long element, FrameState mapState) throws Exception{
        return threshold <= element &&
                mapState.getCount()>0;
    }

    @Override
    protected boolean closePred(long element, FrameState mapState) throws Exception {
        return threshold > element &&
                mapState.getCount()>0;
    }

    @Override
    public void close(long ts, FrameState mapState, Collection<FrameState> resultingFrameState) throws Exception {
        mapState.extend(ts);
        mapState.close();
        resultingFrameState.add(mapState.copy());

        mapState.resetFrameState();
    }

    @Override
    public void open(long ts, long arg, FrameState mapState) throws Exception {
        mapState.setTsStart(ts);
        mapState.increment();
    }

    @Override
    public void update(long ts, long arg, FrameState mapState) throws Exception {
        if (ts >= mapState.getTsEnd()) {
            mapState.extend(ts+1);
        }
        mapState.increment();
    }

    @Override
    protected Collection<FrameState> processOutOfOrder(long ts, long arg, FrameState rebuildingFrameState, Iterable<StreamRecord<I>> iterable) throws Exception {
        //TODO : Fix this artificial solutions
        if (closePred(arg, rebuildingFrameState)){
           FrameState frameState = FrameState.initializeFrameState(rebuildingFrameState.getTsStart(), ts, true);
           frameState.increment(); // This
            // starting time of the next window = ts+1, since otherwise the current event will be part of it
           FrameState frameState1 = FrameState.initializeFrameState(ts+1,rebuildingFrameState.getTsEnd(), true);
           frameState1.increment(); // And This
           return Arrays.asList(frameState, frameState1);
        }
        return Collections.singleton(rebuildingFrameState);

    }

    @Override
    protected Collection<FrameState> recomputeOutOfOrder(long ts, FrameState rebuildingFrameState, Iterable<StreamRecord<I>> iterable) throws Exception {
        //TODO : Fix this artificial solutions

        FrameState frameState = FrameState.initializeFrameState(rebuildingFrameState.getTsStart(), ts, true);
        frameState.increment(); // This
        // starting time of the next window = ts+1, since otherwise the current event will be part of it
        FrameState frameState1 = FrameState.initializeFrameState(ts+1,rebuildingFrameState.getTsEnd(), true);
        frameState1.increment(); // And This
        return Arrays.asList(frameState, frameState1);
    }

}
