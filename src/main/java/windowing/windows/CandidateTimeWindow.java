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

package windowing.windows;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.io.IOException;
import java.io.Serializable;

public class CandidateTimeWindow implements Serializable {

    private final long start;
    private long end;
    private boolean closed;

    public CandidateTimeWindow(long start) {
        this.start = start;
        this.end = start+1; // flink windows are open on the right side, i.e., [2,2) is an empty window
        this.closed = false;
    }

    public CandidateTimeWindow(long start, long end) {
        this.start = start;
        this.end = end;
        this.closed = false;
    }

    public CandidateTimeWindow(long start, long end, boolean closed) {
        this.start = start;
        this.end = end;
        this.closed = closed;
    }

    public void extend(long newEnd){
        this.end = newEnd;
    }

    public TimeWindow close(){
        this.closed = true;
        return new TimeWindow(start, end);
    }

    public long getEnd() {
        return end;
    }

    public long getStart() {
        return start;
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj)
            return true;
        if(obj instanceof CandidateTimeWindow){
            return ((CandidateTimeWindow) obj).end == end && ((CandidateTimeWindow) obj).start == start &&
                    ((CandidateTimeWindow) obj).closed == closed;
        }else return false;
    }

    public TimeWindow getFinalWindow(){
        return new TimeWindow(start, end);
    }

    public long size() {
        // -1 also because the window is open on the right
        return end - start - 1;
    }

    public static class Serializer extends TypeSerializerSingleton<CandidateTimeWindow>{

        @Override
        public boolean isImmutableType() {
            return false;
        }

        @Override
        public CandidateTimeWindow createInstance() {
            return null;
        }

        @Override
        public CandidateTimeWindow copy(CandidateTimeWindow from) {
            return from;
        }

        @Override
        public CandidateTimeWindow copy(CandidateTimeWindow from, CandidateTimeWindow reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return 0;
        }

        @Override
        public void serialize(CandidateTimeWindow record, DataOutputView target) throws IOException {
            target.writeLong(record.start);
            target.writeLong(record.end);
            target.writeBoolean(record.closed);
        }

        @Override
        public CandidateTimeWindow deserialize(DataInputView source) throws IOException {
            long start = source.readLong();
            long dynamicEnd = source.readLong();
            boolean closed = source.readBoolean();
            return new CandidateTimeWindow(start, dynamicEnd, closed);
        }

        @Override
        public CandidateTimeWindow deserialize(CandidateTimeWindow reuse, DataInputView source) throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            target.writeLong(source.readLong());
            target.writeLong(source.readLong());
            target.writeBoolean(source.readBoolean());
        }

        @Override
        public TypeSerializerSnapshot<CandidateTimeWindow> snapshotConfiguration() {
            return new CandidateTimeWindowSerializerSnapshot();
        }

        /**
         * Serializer configuration snapshot for compatibility and format evolution.
         */
        @SuppressWarnings("WeakerAccess")
        public static final class CandidateTimeWindowSerializerSnapshot extends SimpleTypeSerializerSnapshot<CandidateTimeWindow> {

            public CandidateTimeWindowSerializerSnapshot() {
                super(Serializer::new);
            }
        }
    }
}
