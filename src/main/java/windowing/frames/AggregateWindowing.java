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
import java.util.function.BiFunction;
import java.util.function.ToLongFunction;

public class AggregateWindowing<I> extends FrameWindowing<I> {

    private final BiFunction<Long,Long,Long> agg;
    private final Long startValue;
    private final long threshold;

    public AggregateWindowing(BiFunction<Long,Long,Long> agg, Long startValue, Long threshold, ToLongFunction<I> toLongFunctionValue) {
        super(toLongFunctionValue);
        this.threshold = threshold;
        this.agg = agg;
        this.startValue = startValue;
    }

    @Override
    protected boolean openPred(long element, FrameState mapState) throws Exception {
        return mapState.getTsStart()==-1L;
    }

    @Override
    protected boolean updatePred(long element, FrameState mapState) throws Exception{
        return mapState.getAggregate()<threshold;
    }

    @Override
    protected boolean closePred(long element, FrameState mapState) throws Exception {
        return !updatePred(element,mapState);
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
        mapState.setAggregate(agg.apply(arg,startValue));
    }

    @Override
    protected void update(long ts, long arg, FrameState mapState) throws Exception {
        mapState.extend(ts+1);
        mapState.setAggregate(agg.apply(arg, mapState.getAggregate()));
    }

    @Override
    protected Collection<FrameState> processOutOfOrder(long ts, long arg, FrameState rebuildingFrameState) throws Exception {
        //TODO: Add the OOO Processing, probably involving the content from the State backend
        return null;
    }

    @Override
    public Evictor<I, GlobalWindow> evictor() {
        return new Evictor<I, GlobalWindow>() {
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

            }
        };
    }
}
