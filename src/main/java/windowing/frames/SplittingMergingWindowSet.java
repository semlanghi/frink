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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import windowing.windows.DataDrivenWindow;

import java.util.*;
import java.util.stream.Collectors;

public class SplittingMergingWindowSet<W extends Window, P> {

    private static final Logger LOG = LoggerFactory.getLogger(SplittingMergingWindowSet.class);

    /**
     * Mapping from window to the window that keeps the window state. When
     * we are incrementally merging windows starting from some window we keep that starting
     * window as the state window to prevent costly state juggling.
     */
    private final Map<W, W> mapping;
    /**
     * Mapping when we created the {@code MergingWindowSet}. We use this to decide whether
     * we need to persist any changes to state.
     */
    private final Map<W, W> initialMapping;

    private final ListState<Tuple2<W, W>> state;

    /**
     * Our window assigner.
     */
    private final MergingWindowAssigner<?, W> windowAssigner;
    private int size;

    public int size() {
        return size;
    }

    /**
     * Restores a {@link SplittingMergingWindowSet} from the given state.
     */
    public SplittingMergingWindowSet(MergingWindowAssigner<?, W> windowAssigner, ListState<Tuple2<W, W>> state) throws Exception {
        this.windowAssigner = windowAssigner;
        mapping = new HashMap<>();

        Iterable<Tuple2<W, W>> windowState = state.get();
        if (windowState != null) {
            for (Tuple2<W, W> window : windowState) {
                mapping.put(window.f0, window.f1);
            }
        }

        this.state = state;
        this.size = 0;
//        state.get().iterator().forEachRemaining(w -> size++);

        initialMapping = new HashMap<>();
        initialMapping.putAll(mapping);
    }

    /**
     * Persist the updated mapping to the given state if the mapping changed since
     * initialization.
     */
    public void persist() throws Exception {
        if (!mapping.equals(initialMapping)) {
            state.clear();
            for (Map.Entry<W, W> window : mapping.entrySet()) {
                state.add(new Tuple2<>(window.getKey(), window.getValue()));
            }
        }
    }

    /**
     * Returns the state window for the given in-flight {@code Window}. The state window is the
     * {@code Window} in which we keep the actual state of a given in-flight window. Windows
     * might expand but we keep to original state window for keeping the elements of the window
     * to avoid costly state juggling.
     *
     * @param window The window for which to get the state window.
     */
    public W getStateWindow(W window) {
        return mapping.get(window);
    }

    /**
     * Removes the given window from the set of in-flight windows.
     *
     * @param window The {@code Window} to remove.
     */
    public void retireWindow(W window) {
        W removed = this.mapping.remove(window);
        if (removed == null) {
            throw new IllegalStateException("Window " + window + " is not in in-flight window set.");
        }
    }

    public void splitMergedWindow(W targetWindow, List<Pair<P, W>> splittingPointStateWindows, Splitter<W, P> splitter, boolean leaveLastWindowClosed) {

        /*
        First thing: remove the target window, due to the asymmetry between merging and state windows we cannot use
        its mapped state window, since in the state window the events contained are completely independent to
        its boundaries, e.g., [1000,1001] -> {(event1, ts:1000), (event2, ts: 3000)
         */
        this.mapping.remove(targetWindow);
        this.size--;

        W focusedWindow = targetWindow;
        W previousWindow = null;

        /*
        Splitting the target window, placing the first of the splitted halves in the mapping, together with the
        correlated state window, then shift the focus to the second half
        NOTE: the state window should correspond to the elements BEFORE the splitting point
        e.g. 2000 -> [1000,1001] -> (WindowMergingState) {1000,1050,1999}
         */
        splittingPointStateWindows.sort(Comparator.comparingLong(o -> o.getRight().maxTimestamp()));
        for (Pair<P, W> tmp : splittingPointStateWindows) {
            Pair<W, W> splittedRemains = splitter.split(focusedWindow, tmp.getLeft());
            //Check for threshold window
            if (((TimeWindow) splittedRemains.getLeft()).getStart() == ((TimeWindow) tmp.getRight()).getStart()) {
                this.mapping.put(splittedRemains.getLeft(), tmp.getRight());
                this.size++;
            } else {
                this.mapping.put(tmp.getRight(), tmp.getRight());
                this.size++;
            }
            previousWindow = splittedRemains.getLeft();
            focusedWindow = splittedRemains.getRight();
        }
        if (previousWindow instanceof DataDrivenWindow)
            ((DataDrivenWindow) previousWindow).setClosed(leaveLastWindowClosed);
    }

    public void mergeRebalancing(W focusedWindow, MergeFunction<W> mergeFunction) throws Exception {

        List<W> windows = new ArrayList<>();
        windows.add(focusedWindow);
        windows.addAll(this.mapping.keySet().stream().filter(w -> w.maxTimestamp() > focusedWindow.maxTimestamp()).collect(Collectors.toList()));

        final Map<W, Collection<W>> mergeResults = new HashMap<>();
        windowAssigner.mergeWindows(windows,
                (toBeMerged, mergeResult) -> {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Merging {} into {}", toBeMerged, mergeResult);
                    }
                    mergeResults.put(mergeResult, toBeMerged);
                });

        // perform the merge
        for (Map.Entry<W, Collection<W>> c : mergeResults.entrySet()) {
            W mergeResult = c.getKey();
            Collection<W> mergedWindows = c.getValue();

            // pick any of the merged windows and choose that window's state window
            // as the state window for the merge result
            W mergedStateWindow = null;

            // figure out the state windows that we are merging
            List<W> mergedStateWindows = new ArrayList<>();
            for (W mergedWindow : mergedWindows) {
                W res = this.mapping.remove(mergedWindow);
                this.size--;
                if (res != null) {
                    if (mergedStateWindow != null) {
                        if (mergedStateWindow.maxTimestamp() > res.maxTimestamp()) {
                            mergedStateWindows.add(mergedStateWindow);
                            mergedStateWindow = res;
                        } else mergedStateWindows.add(res);
                    } else mergedStateWindow = res;
                }
            }

            this.mapping.put(mergeResult, mergedStateWindow);
            this.size++;
            if (!(mergedWindows.size() == 1)) {
                mergeFunction.merge(mergeResult,
                        mergedWindows,
                        mergedStateWindow,
                        mergedStateWindows);
            }
        }
    }

    /**
     * Adds a new {@code Window} to the set of in-flight windows. It might happen that this
     * triggers merging of previously in-flight windows. In that case, the provided
     * {@link MergeFunction} is called.
     *
     * <p>This returns the window that is the representative of the added window after adding.
     * This can either be the new window itself, if no merge occurred, or the newly merged
     * window. Adding an element to a window or calling singleBufferTrigger functions should only
     * happen on the returned representative. This way, we never have to deal with a new window
     * that is immediately swallowed up by another window.
     *
     * <p>If the new window is merged, the {@code MergeFunction} callback arguments also don't
     * contain the new window as part of the list of merged windows.
     *
     * @param newWindow     The new {@code Window} to add.
     * @param mergeFunction The callback to be invoked in case a merge occurs.
     * @return The {@code Window} that new new {@code Window} ended up in. This can also be the
     * the new {@code Window} itself in case no merge occurred.
     * @throws Exception
     */
    public W addWindow(W newWindow, MergeFunction<W> mergeFunction) throws Exception {

        List<W> windows = new ArrayList<>();

        windows.addAll(this.mapping.keySet());
        windows.add(newWindow);

        final Map<W, Collection<W>> mergeResults = new HashMap<>();
        windowAssigner.mergeWindows(windows,
                (toBeMerged, mergeResult) -> {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Merging {} into {}", toBeMerged, mergeResult);
                    }
                    mergeResults.put(mergeResult, toBeMerged);
                });

        W resultWindow = newWindow;
        boolean mergedNewWindow = false;

        // perform the merge
        for (Map.Entry<W, Collection<W>> c : mergeResults.entrySet()) {
            W mergeResult = c.getKey();
            Collection<W> mergedWindows = c.getValue();

            // if our new window is in the merged windows make the merge result the
            // result window
            if (mergedWindows.remove(newWindow)) {
                mergedNewWindow = true;
                resultWindow = mergeResult;
            }

            // pick any of the merged windows and choose that window's state window
            // as the state window for the merge result
            W mergedStateWindow = this.mapping.get(mergedWindows.iterator().next());

            // figure out the state windows that we are merging
            List<W> mergedStateWindows = new ArrayList<>();
            for (W mergedWindow : mergedWindows) {
                W res = this.mapping.remove(mergedWindow);
                this.size--;
                if (res != null) {
                    mergedStateWindows.add(res);
                }
            }

            this.mapping.put(mergeResult, mergedStateWindow);
            this.size++;
            // don't put the target state window into the merged windows
            mergedStateWindows.remove(mergedStateWindow);

            // don't merge the new window itself, it never had any state associated with it
            // i.e. if we are only merging one pre-existing window into itself
            // without extending the pre-existing window
            if (!(mergedWindows.contains(mergeResult) && mergedWindows.size() == 1)) {
                mergeFunction.merge(mergeResult,
                        mergedWindows,
                        this.mapping.get(mergeResult),

                        mergedStateWindows);
            }
        }

        // the new window created a new, self-contained window without merging
        if (mergeResults.isEmpty() || (resultWindow.equals(newWindow) && !mergedNewWindow)) {
            this.mapping.put(resultWindow, resultWindow);
            this.size++;
        }

        return resultWindow;
    }

    /**
     * Callback for {@link #addWindow(Window, MergeFunction)}.
     *
     * @param <W>
     */
    public interface MergeFunction<W> {

        /**
         * This gets called when a merge occurs.
         *
         * @param mergeResult        The newly resulting merged {@code Window}.
         * @param mergedWindows      The merged {@code Window Windows}.
         * @param stateWindowResult  The state window of the merge result.
         * @param mergedStateWindows The merged state windows.
         * @throws Exception
         */
        void merge(W mergeResult, Collection<W> mergedWindows, W stateWindowResult, Collection<W> mergedStateWindows) throws Exception;
    }

    public Collection<W> getKeys() {
        return mapping.values();
    }

    @Override
    public String toString() {
        return "MergingWindowSet{" +
               "windows=" + mapping +
               '}';
    }
}
