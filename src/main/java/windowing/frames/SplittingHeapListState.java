package windowing.frames;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMergingState;

import java.util.*;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

public class SplittingHeapListState<K, N, V> implements InternalSplitAndMergeState<K, N, V, List<V>, Iterable<V>> {

    protected final InternalListState<K, N, V> internalMergingState;

//    /** The current namespace, which the access methods will refer to. */
//    protected N currentNamespace;
//
//    protected final TypeSerializer<K> keySerializer;
//
//    protected final TypeSerializer<List<V>> valueSerializer;
//
//    protected final TypeSerializer<N> namespaceSerializer;
//
//    private final List<V> defaultValue;
//
//    private final MergeTransformation mergeTransformation;


    protected SplittingHeapListState(
            InternalMergingState<K, N, V, List<V>, Iterable<V>> heapListState) {
        this.internalMergingState = (InternalListState<K, N, V>) heapListState;
//        this.stateTable = Preconditions.checkNotNull(, "State table must not be null.");
//        this.keySerializer = keySerializer;
//        this.valueSerializer = valueSerializer;
//        this.namespaceSerializer = namespaceSerializer;
//        this.defaultValue = defaultValue;
//        this.currentNamespace = null;
//        this.mergeTransformation = new MergeTransformation();
    }

    @Override
    public int size() {
        try {
            return internalMergingState.getInternal().size();
        } catch (Exception e) {
            return -1;
        }
    }


    @Override
    public TypeSerializer<K> getKeySerializer() {
        return this.internalMergingState.getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return internalMergingState.getNamespaceSerializer();
    }

    @Override
    public TypeSerializer<List<V>> getValueSerializer() {
        return internalMergingState.getValueSerializer();
    }

    @Override
    public final void setCurrentNamespace(N namespace) {
        internalMergingState.setCurrentNamespace(namespace);
    }

    @Override
    public byte[] getSerializedValue(
            final byte[] serializedKeyAndNamespace,
            final TypeSerializer<K> safeKeySerializer,
            final TypeSerializer<N> safeNamespaceSerializer,
            final TypeSerializer<List<V>> safeValueSerializer) throws Exception {

        return internalMergingState.getSerializedValue(
                serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer, safeValueSerializer);
    }

    @Override
    public StateIncrementalVisitor<K, N, List<V>> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
        return internalMergingState.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }

    @Override
    public final void clear() {
        internalMergingState.clear();
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
        internalMergingState.mergeNamespaces(target, sources);
    }

    @Override
    public List<V> getInternal() {
        try {
            return internalMergingState.getInternal();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void updateInternal(List<V> valueToStore) throws Exception {

    }

    @Override
    public Iterable<V> get() throws Exception {
        return getInternal();
    }

    @Override
    public void add(V value) throws Exception {
        internalMergingState.add(value);
    }


    final class MergeTransformation implements StateTransformationFunction<List<V>, List<V>> {

        @Override
        public List<V> apply(List<V> targetState, List<V> merged) throws Exception {
            if (targetState != null) {
                return mergeState(targetState, merged);
            } else {
                return merged;
            }
        }
    }

    protected List<V> mergeState(List<V> a, List<V> b) {
        a.addAll(b);
        return a;
    }

    public void splitNamespaces(N target, Collection<N> splittedTargets, BiPredicate<V, N> matchingPredicate) {

        // Take and remove the old window metadata
        try {
            internalMergingState.setCurrentNamespace(target);
            final List<V> oldElementSet = internalMergingState.getInternal();

            internalMergingState.clear();

            // If not element set is found the windows are new, so do nothing
            if (oldElementSet == null)
                return;

            // For all the namespaces, search the corresponding elements, then add it to the state table
            splittedTargets.stream().forEach(n -> {
                try {
                    internalMergingState.setCurrentNamespace(n);
                    internalMergingState.addAll(oldElementSet.stream()
                            .filter(v -> matchingPredicate.test(v, n))
                            .collect(Collectors.toList()));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
