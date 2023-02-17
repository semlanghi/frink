package windowing.frames;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class StateAwareTriggerContextWrapper<T, W extends Window> implements StateAwareContextWrapper<T, W> {

    private StateAwareSingleBufferWindowOperator<?,T,?,W>.StateAwareContext internalContext;

    public StateAwareTriggerContextWrapper(Trigger.TriggerContext internalContext) {
        this.internalContext = (StateAwareSingleBufferWindowOperator<?, T, ?, W>.StateAwareContext) internalContext;
    }

    @Override
    public ValueState<FrameState> getCurrentFrameState(ValueStateDescriptor<FrameState> stateDescriptor) {
        return internalContext.getCurrentFrameState(stateDescriptor);
    }

    @Override
    public MapState<Long, FrameState> getPastFrameState(MapStateDescriptor<Long, FrameState> stateDescriptor) {
        return internalContext.getPastFrameState(stateDescriptor);
    }

    @Override
    public Iterable<StreamRecord<T>> getContent(W window) {
        return internalContext.getContent(window);
    }

    @Override
    public long getCurrentWatermark() {
        return internalContext.getCurrentWatermark();
    }

    @Override
    public long getAllowedLateness() {
        return internalContext.getAllowedLateness();
    }
}
