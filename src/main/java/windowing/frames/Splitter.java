package windowing.frames;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.Collection;

@FunctionalInterface
public interface Splitter<W extends Window,P> {
    public Pair<W,W> split(W target, P splittingPoint);
}
