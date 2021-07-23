package windowing;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.windowing.AggregateApplyWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ReduceApplyWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.BaseAlignedWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.EvictingWindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalSingleValueWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.lang.reflect.Type;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class StateAwareWindowedStream<T, K, W extends Window> extends WindowedStream<T, K, W> {

    private KeyedStream<T,K> visibleInput;
    private WindowAssigner<T,W> visibleWindowAssigner;
    private Trigger<T,W> visibleTrigger;
    private Evictor<T,W> visibleEvictor;
    private long visibleAllowedLateness;
    private OutputTag<T> visibleLateDataOutputTag;

    @PublicEvolving
    public WindowedStream<T, K, W> allowedLateness(Time lateness) {
        visibleAllowedLateness = lateness.toMilliseconds();
        return this;
    }

    public StateAwareWindowedStream(KeyedStream<T, K> input, WindowAssigner<? super T, W> windowAssigner) {
        super(input, windowAssigner);
        this.visibleInput = input;
        this.visibleWindowAssigner = (WindowAssigner<T, W>) windowAssigner;
        this.visibleTrigger = (Trigger<T, W>) windowAssigner.getDefaultTrigger(input.getExecutionEnvironment());
    }

    public StateAwareWindowedStream(KeyedStream<T, K> input, WindowAssigner<T, W> windowAssigner, Trigger<T,W> trigger, Evictor<T,W> evictor) {
        super(input, windowAssigner);
        this.visibleInput = input;
        this.visibleWindowAssigner = windowAssigner;
        this.visibleTrigger = trigger;
        this.visibleEvictor = evictor;
    }


    @Override
    public <ACC, V, R> SingleOutputStreamOperator<R> aggregate(
            AggregateFunction<T, ACC, V> aggregateFunction,
            WindowFunction<V, R, K, W> windowFunction,
            TypeInformation<ACC> accumulatorType,
            TypeInformation<R> resultType) {

        checkNotNull(aggregateFunction, "aggregateFunction");
        checkNotNull(windowFunction, "windowFunction");
        checkNotNull(accumulatorType, "accumulatorType");
        checkNotNull(resultType, "resultType");

        if (aggregateFunction instanceof RichFunction) {
            throw new UnsupportedOperationException("This aggregate function cannot be a RichFunction.");
        }

        //clean the closures
        windowFunction = visibleInput.getExecutionEnvironment().clean(windowFunction);
        aggregateFunction = visibleInput.getExecutionEnvironment().clean(aggregateFunction);

        final String opName = generateOperatorName(visibleWindowAssigner, visibleTrigger, visibleEvictor, aggregateFunction, windowFunction);
        KeySelector<T, K> keySel = visibleInput.getKeySelector();

        OneInputStreamOperator<T, R> operator;

        if (visibleEvictor != null) {
            @SuppressWarnings({"unchecked", "rawtypes"})
            TypeSerializer<StreamRecord<T>> streamRecordSerializer =
                    (TypeSerializer<StreamRecord<T>>) new StreamElementSerializer(visibleInput.getType().createSerializer(getExecutionEnvironment().getConfig()));

            ListStateDescriptor<StreamRecord<T>> stateDesc =
                    new ListStateDescriptor<>("window-contents", streamRecordSerializer);

            operator = new EvictingWindowOperator<>(visibleWindowAssigner,
                    visibleWindowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
                    keySel,
                    visibleInput.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
                    stateDesc,
                    new InternalIterableWindowFunction<>(new AggregateApplyWindowFunction<>(aggregateFunction, windowFunction)),
                    visibleTrigger,
                    visibleEvictor,
                    visibleAllowedLateness,
                    visibleLateDataOutputTag);

        } else {
            AggregatingStateDescriptor<T, ACC, V> stateDesc = new AggregatingStateDescriptor<>("window-contents",
                    aggregateFunction, accumulatorType.createSerializer(getExecutionEnvironment().getConfig()));

            operator = new WindowOperator<>(visibleWindowAssigner,
                    visibleWindowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
                    keySel,
                    visibleInput.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
                    stateDesc,
                    new InternalSingleValueWindowFunction<>(windowFunction),
                    visibleTrigger,
                    visibleAllowedLateness,
                    visibleLateDataOutputTag);
        }

        return visibleInput.transform(opName, resultType, operator);
    }

    @PublicEvolving
    public WindowedStream<T, K, W> sideOutputLateData(OutputTag<T> outputTag) {
        Preconditions.checkNotNull(outputTag, "Side output tag must not be null.");
        this.visibleLateDataOutputTag = visibleInput.getExecutionEnvironment().clean(outputTag);
        super.sideOutputLateData(outputTag);
        return this;
    }

    private static String generateOperatorName(
            WindowAssigner<?, ?> assigner,
            Trigger<?, ?> trigger,
            @Nullable Evictor<?, ?> evictor,
            Function function1,
            @Nullable Function function2) {
        return "Window(" +
                assigner + ", " +
                trigger.getClass().getSimpleName() + ", " +
                (evictor == null ? "" : (evictor.getClass().getSimpleName() + ", ")) +
                generateFunctionName(function1) +
                (function2 == null ? "" : (", " + generateFunctionName(function2))) +
                ")";
    }

    private static String generateFunctionName(Function function) {
        Class<? extends Function> functionClass = function.getClass();
        if (functionClass.isAnonymousClass()) {
            // getSimpleName returns an empty String for anonymous classes
            Type[] interfaces = functionClass.getInterfaces();
            if (interfaces.length == 0) {
                // extends an existing class (like RichMapFunction)
                Class<?> functionSuperClass = functionClass.getSuperclass();
                return functionSuperClass.getSimpleName() + functionClass.getName().substring(functionClass.getEnclosingClass().getName().length());
            } else {
                // implements a Function interface
                Class<?> functionInterface = functionClass.getInterfaces()[0];
                return functionInterface.getSimpleName() + functionClass.getName().substring(functionClass.getEnclosingClass().getName().length());
            }
        } else {
            return functionClass.getSimpleName();
        }
    }

    public <R> SingleOutputStreamOperator<R> reduce(
            ReduceFunction<T> reduceFunction,
            WindowFunction<T, R, K, W> function,
            TypeInformation<R> resultType) {

        if (reduceFunction instanceof RichFunction) {
            throw new UnsupportedOperationException("ReduceFunction of reduce can not be a RichFunction.");
        }

        //clean the closures
        function = visibleInput.getExecutionEnvironment().clean(function);
        reduceFunction = visibleInput.getExecutionEnvironment().clean(reduceFunction);

        final String opName = generateOperatorName(visibleWindowAssigner, visibleTrigger, visibleEvictor, reduceFunction, function);
        KeySelector<T, K> keySel = visibleInput.getKeySelector();

        OneInputStreamOperator<T, R> operator;


        if (visibleEvictor != null) {
            @SuppressWarnings({"unchecked", "rawtypes"})
            TypeSerializer<StreamRecord<T>> streamRecordSerializer =
                    (TypeSerializer<StreamRecord<T>>) new StreamElementSerializer(visibleInput.getType().createSerializer(getExecutionEnvironment().getConfig()));

            ListStateDescriptor<StreamRecord<T>> stateDesc =
                    new ListStateDescriptor<>("window-contents", streamRecordSerializer);

            operator =
                    new StateAwareSingleBufferWindowOperator<>(visibleWindowAssigner,
                            visibleWindowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
                            keySel,
                            visibleInput.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
                            stateDesc,
                            new InternalIterableWindowFunction<>(new ReduceApplyWindowFunction<>(reduceFunction, function)),
                            visibleTrigger,
                            visibleEvictor,
                            visibleAllowedLateness,
                            visibleLateDataOutputTag);

        } else {
            TypeSerializer<StreamRecord<T>> streamRecordSerializer =
                    (TypeSerializer<StreamRecord<T>>) new StreamElementSerializer(visibleInput.getType().createSerializer(getExecutionEnvironment().getConfig()));


//            ReducingStateDescriptor<StreamRecord<T>> stateDesc = new ReducingStateDescriptor<>("window-contents",
//                    reduceFunction, streamRecordSerializer);

            ListStateDescriptor<StreamRecord<T>> stateDesc =
                    new ListStateDescriptor<>("window-contents", streamRecordSerializer);
            operator =
                    new StateAwareMultiBufferWindowOperator<>(visibleWindowAssigner,
                            visibleWindowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
                            keySel,
                            visibleInput.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
                            stateDesc,
                            new InternalIterableWindowFunction<>(new ReduceApplyWindowFunction<>(reduceFunction, function)),
                            visibleTrigger,
                            30000L,
                            visibleLateDataOutputTag);
        }


        return visibleInput.transform(opName, resultType, operator);
    }


    @Override
    public StateAwareWindowedStream<T, K, W> evictor(Evictor<? super T, ? super W> evictor) {
        if (visibleWindowAssigner instanceof BaseAlignedWindowAssigner) {
            throw new UnsupportedOperationException("Cannot use a " + visibleWindowAssigner.getClass().getSimpleName() + " with an Evictor.");
        }
        this.visibleEvictor = (Evictor<T, W>) evictor;
        return this;
    }

    @Override
    public StateAwareWindowedStream<T, K, W> trigger(Trigger<? super T, ? super W> trigger) {
        if (visibleWindowAssigner instanceof MergingWindowAssigner && !trigger.canMerge()) {
            throw new UnsupportedOperationException("A merging window assigner cannot be used with a trigger that does not support merging.");
        }

        if (visibleWindowAssigner instanceof BaseAlignedWindowAssigner) {
            throw new UnsupportedOperationException("Cannot use a " + visibleWindowAssigner.getClass().getSimpleName() + " with a custom trigger.");
        }

        this.visibleTrigger = (Trigger<T, W>) trigger;
        return this;
    }
}
