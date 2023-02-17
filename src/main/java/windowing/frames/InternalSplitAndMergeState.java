package windowing;

import org.apache.flink.runtime.state.internal.InternalMergingState;

import java.util.Collection;
import java.util.function.BiPredicate;

public interface InternalSplitAndMergeState<K, N, V, SV, OUT> extends InternalMergingState<K, N, V, SV, OUT> {

    public void splitNamespaces(N target, Collection<N> splittedTargets, BiPredicate<V, N> matchingPredicate);
}
