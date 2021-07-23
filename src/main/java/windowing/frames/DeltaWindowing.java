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

import java.util.Collection;
import java.util.Iterator;
import java.util.function.ToLongFunction;

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
        return Math.abs(mapState.getAuxiliaryValue()-element) > threshold;
    }

    @Override
    protected boolean closePred(long element, FrameState mapState) throws Exception {
        return Math.abs(mapState.getAuxiliaryValue()-element) <= threshold;
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
    protected Collection<FrameState> processOutOfOrder(long ts, long arg, FrameState rebuildingFrameState) throws Exception {
        // TODO: complete this method with the OOO processing, probably involving the content of previous elements
        return null;
    }

    @Override
    public Evictor<I, GlobalWindow> evictor() {
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