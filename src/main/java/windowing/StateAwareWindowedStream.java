package windowing;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.windowing.PassThroughWindowFunction;
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
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;
import windowing.frames.FrameWindowing;
import windowing.frames.StateAwareMultiBufferWindowOperator;
import windowing.frames.StateAwareSingleBufferWindowOperator;
import windowing.time.FrinkSlidingEventTimeWindows;
import windowing.time.FrinkTimeBasedMultiBufferWindowOperator;
import windowing.time.FrinkTimeBasedSingleBufferWindowOperator;

import javax.annotation.Nullable;
import java.lang.reflect.Type;

public class StateAwareWindowedStream<T, K, W extends Window> extends WindowedStream<T, K, W> {

    private KeyedStream<T, K> visibleInput;
    private WindowAssigner<T, W> visibleWindowAssigner;
    private Trigger<T, W> visibleTrigger;
    private Evictor<T, W> visibleEvictor;
    private long visibleAllowedLateness;
    private OutputTag<T> visibleLateDataOutputTag;

    public StateAwareWindowedStream(KeyedStream<T, K> input, WindowAssigner<T, W> windowAssigner) {
        super(input, windowAssigner);
        this.visibleInput = input;
        this.visibleWindowAssigner = windowAssigner;
        this.visibleTrigger = windowAssigner.getDefaultTrigger(input.getExecutionEnvironment());
    }

    @PublicEvolving
    public StateAwareWindowedStream<T, K, W> allowedLateness(Time lateness) {
        visibleAllowedLateness = lateness.toMilliseconds();
        return this;
    }

    @PublicEvolving
    public StateAwareWindowedStream<T, K, W> sideOutputLateData(OutputTag<T> outputTag) {
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

        TypeSerializer<StreamRecord<T>> streamRecordSerializer =
                (TypeSerializer<StreamRecord<T>>) new StreamElementSerializer(visibleInput.getType().createSerializer(getExecutionEnvironment().getConfig()));

        ListStateDescriptor<StreamRecord<T>> stateDesc =
                new ListStateDescriptor<>("window-contents", streamRecordSerializer);

        if (this.isFrame()) {//Multi-Buffer data-driven windowing
                operator =
                        new StateAwareMultiBufferWindowOperator<>(visibleWindowAssigner,
                                visibleWindowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
                                keySel,
                                visibleInput.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
                                stateDesc,
                                new InternalIterableWindowFunction<>(new ReduceApplyWindowFunction<>(reduceFunction, function)),
                                visibleTrigger,
                                visibleAllowedLateness,
                                visibleLateDataOutputTag);

        } else if (this.isTime()){ // Multi-Buffer Time Based Windowing
                operator =
                        new FrinkTimeBasedMultiBufferWindowOperator<>(
                                visibleWindowAssigner,
                                visibleWindowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
                                keySel,
                                visibleInput.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
                                stateDesc,
                                new InternalIterableWindowFunction<>(new ReduceApplyWindowFunction<>(reduceFunction, function)),
                                visibleTrigger,
                                visibleAllowedLateness,
                                visibleLateDataOutputTag);
        } else if (visibleEvictor != null){ //Single-buffer

             if (visibleTrigger instanceof FrameWindowing.FrameTrigger){
                    operator = new StateAwareSingleBufferWindowOperator<>(visibleWindowAssigner,
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
                 operator =
                         new FrinkTimeBasedSingleBufferWindowOperator<>(visibleWindowAssigner,
                                 visibleWindowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
                                 keySel,
                                 visibleInput.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
                                 stateDesc,
                                 new InternalIterableWindowFunction<>(new ReduceApplyWindowFunction<>(reduceFunction, function)),
                                 visibleTrigger,
                                 visibleEvictor,
                                 visibleAllowedLateness,
                                 visibleLateDataOutputTag);
             }

        } else throw new RuntimeException("Windowing setup not correct.");
        return visibleInput.transform(opName, resultType, operator);
    }

    private boolean isFrame() {
        return visibleWindowAssigner instanceof FrameWindowing;
    }

    private boolean isTime() {
        return visibleWindowAssigner instanceof FrinkSlidingEventTimeWindows;
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
