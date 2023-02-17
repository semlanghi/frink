package windowing.frames;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import windowing.frames.FrameState;
import windowing.frames.StateAwareMultiBufferWindowOperator;

import java.io.Serializable;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;

/**
 * An abstract class to indicate window assigner that maintains a view on the state backend
 * by extending the {@link org.apache.flink.streaming.api.windowing.assigners.WindowAssigner.WindowAssignerContext} class
 * and instantiating it in the {@link StateAwareMultiBufferWindowOperator#open()} method. This way we give access through a closure
 * to the window operator state backend.
 * @param <T> the class of the managed record
 * @param <W> the window assigned
 */
public abstract class StateAwareWindowAssigner<T,W extends Window> extends MergingWindowAssigner<T,W> implements Serializable {

    public abstract BiFunction<?,?,W> getWindowFactory();

    public abstract BiPredicate<StreamRecord<T>,W> getWindowMatcher();

    public abstract static class StateAwareWindowAssignerContext<T, W extends Window> extends WindowAssignerContext{

        public abstract ValueState<FrameState> getCurrentFrameState(ValueStateDescriptor<FrameState> stateDescriptor);

        public abstract MapState<Long, FrameState> getPastFrameState(MapStateDescriptor<Long, FrameState> stateDescriptor);

        public abstract Iterable<StreamRecord<T>> getContent(W window);

        public abstract long getCurrentWatermark();

        public abstract long getAllowedLateness();
    }
}
