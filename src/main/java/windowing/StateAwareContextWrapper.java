package windowing;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import windowing.frames.FrameState;

public interface StateAwareContextWrapper<T, W extends Window> {

    public ValueState<FrameState> getCurrentFrameState(ValueStateDescriptor<FrameState> stateDescriptor);

    public MapState<Long, FrameState> getPastFrameState(MapStateDescriptor<Long, FrameState> stateDescriptor);

    public Iterable<StreamRecord<T>> getContent(W window);
}
