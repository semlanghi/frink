package windowing.frames;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.io.Serializable;

public class FrameState implements Serializable {

    // TODO: Convert this class into a gateway class towards the State Backend, it is more efficient, you don't have to (de)serialize the whole state
    private long count;
    private long tsStart;
    private long tsEnd;
    private boolean isClosed;
    private long auxiliaryValue;
    private long aggregate;

    public FrameState(long count, long tsStart, long auxiliaryValue, long aggregate, long tsEnd, boolean isClosed) {
        this.count = count;
        this.tsStart = tsStart;
        this.auxiliaryValue = auxiliaryValue;
        this.aggregate = aggregate;
        this.tsEnd = tsEnd;
        this.isClosed = isClosed;
    }

    public static FrameState initializeFrameState(long tsStart){
        return new FrameState(0L, tsStart, 0L, 0L, tsStart+1, false);
    }

    public static FrameState initializeFrameState(long tsStart, long tsEnd, boolean isClosed){
        return new FrameState(0L, tsStart, 0L, 0L, tsEnd, isClosed);
    }

    public void resetFrameState(){
        this.count = 0L;
        this.tsStart = -1L;
        this.auxiliaryValue = 0L;
        this.aggregate = 0L;
        this.tsEnd = 1;
        this.isClosed = false;
    }

    public long getCount() {
        return count;
    }

    public boolean isClosed() {
        return isClosed;
    }

    public void close(){
        this.isClosed = true;
    }

    public void increment(){
        count++;
    }

    public void resetCounter(){
        count = 0L;
    }

    public long getTsStart() {
        return tsStart;
    }

    public void setTsStart(long tsStart) {
        this.tsStart = tsStart;
        this.tsEnd = tsStart+1;
    }

    public long getTsEnd() {
        return tsEnd;
    }

    public void extend(long tsEnd) {
        this.tsEnd = tsEnd;
    }

    public long getAuxiliaryValue() {
        return auxiliaryValue;
    }

    public void setAuxiliaryValue(long auxiliaryValue){
        this.auxiliaryValue = auxiliaryValue;
    }

    public long getAggregate() {
        return aggregate;
    }

    public void setAggregate(long aggregate) {
        this.aggregate = aggregate;
    }

    public FrameState copy(){
        return new FrameState(count, tsStart, auxiliaryValue, aggregate, tsEnd, isClosed);
    }


    public static class Serializer extends TypeSerializerSingleton<FrameState>{

        @Override
        public boolean isImmutableType() {
            return false;
        }

        @Override
        public FrameState createInstance() {
            return null;
        }

        @Override
        public FrameState copy(FrameState from) {
            return from;
        }

        @Override
        public FrameState copy(FrameState from, FrameState reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return 0;
        }

        @Override
        public void serialize(FrameState record, DataOutputView target) throws IOException {
            target.writeLong(record.count);
            target.writeLong(record.tsStart);
            target.writeLong(record.auxiliaryValue);
            target.writeLong(record.aggregate);
            target.writeLong(record.tsEnd);
            target.writeBoolean(record.isClosed);
        }

        @Override
        public FrameState deserialize(DataInputView source) throws IOException {
            long count = source.readLong();
            long tsStart = source.readLong();
            long auxiliaryValue = source.readLong();
            long aggregate = source.readLong();
            long tsEnd = source.readLong();
            boolean isClosed = source.readBoolean();
            return new FrameState(count, tsStart, auxiliaryValue, aggregate, tsEnd, isClosed);
        }

        @Override
        public FrameState deserialize(FrameState reuse, DataInputView source) throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            target.writeLong(source.readLong());
            target.writeLong(source.readLong());
            target.writeLong(source.readLong());
            target.writeLong(source.readLong());
            target.writeLong(source.readLong());
            target.writeBoolean(source.readBoolean());
        }

        @Override
        public TypeSerializerSnapshot<FrameState> snapshotConfiguration() {
            return new FrameStateSerializerSnapshot();
        }


        public static final class FrameStateSerializerSnapshot extends SimpleTypeSerializerSnapshot<FrameState> {

            public FrameStateSerializerSnapshot() {
                super(Serializer::new);
            }
        }
    }
}
