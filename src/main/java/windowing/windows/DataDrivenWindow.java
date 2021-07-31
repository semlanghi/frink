package windowing.windows;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.io.IOException;
import java.util.Objects;

public class DataDrivenWindow extends TimeWindow {

    private boolean isClosed;

    public DataDrivenWindow(long start, long end, boolean isClosed) {
        super(start, end);
        this.isClosed = isClosed;
    }

    public boolean isClosed() {
        return isClosed;
    }

    public DataDrivenWindow cover(DataDrivenWindow other) {
        return new DataDrivenWindow(Math.min(this.getStart(), other.getStart()), Math.max(this.getEnd(), other.getEnd()), isClosed || other.isClosed);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DataDrivenWindow)) return false;
        TimeWindow that = (TimeWindow) o;
        return this.getStart() == that.getStart() && this.getEnd()  == that.getEnd();
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode());
    }

    @Override
    public String toString() {
        return "DataDrivenWindow{" +
                "tsStart=" + this.getStart()+", "+
                "tsEnd=" + this.getEnd()+", "+
                "isClosed=" + isClosed +
                '}';
    }

    public static class Serializer extends TypeSerializerSingleton<DataDrivenWindow> {

        @Override
        public boolean isImmutableType() {
            return false;
        }

        @Override
        public DataDrivenWindow createInstance() {
            return null;
        }

        @Override
        public DataDrivenWindow copy(DataDrivenWindow from) {
            return from;
        }

        @Override
        public DataDrivenWindow copy(DataDrivenWindow from, DataDrivenWindow reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return 0;
        }

        @Override
        public void serialize(DataDrivenWindow record, DataOutputView target) throws IOException {
            target.writeLong(record.getStart());
            target.writeLong(record.getEnd());
            target.writeBoolean(record.isClosed);
        }

        @Override
        public DataDrivenWindow deserialize(DataInputView source) throws IOException {
            long start = source.readLong();
            long dynamicEnd = source.readLong();
            boolean closed = source.readBoolean();
            return new DataDrivenWindow(start, dynamicEnd, closed);
        }

        @Override
        public DataDrivenWindow deserialize(DataDrivenWindow reuse, DataInputView source) throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            target.writeLong(source.readLong());
            target.writeLong(source.readLong());
            target.writeBoolean(source.readBoolean());
        }

        @Override
        public TypeSerializerSnapshot<DataDrivenWindow> snapshotConfiguration() {
            return new DataDrivenWindow.Serializer.DataDrivenWindowSerializerSnapshot();
        }

        /**
         * Serializer configuration snapshot for compatibility and format evolution.
         */
        @SuppressWarnings("WeakerAccess")
        public static final class DataDrivenWindowSerializerSnapshot extends SimpleTypeSerializerSnapshot<DataDrivenWindow> {

            public DataDrivenWindowSerializerSnapshot() {
                super(DataDrivenWindow.Serializer::new);
            }
        }
    }
}
