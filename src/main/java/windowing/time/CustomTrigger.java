package windowing.time;

import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import windowing.ComplexTriggerResult;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class CustomTrigger<I, W extends TimeWindow> extends Trigger<I, GlobalWindow> {

    private WindowAssigner<I, W> wa;

    public CustomTrigger(WindowAssigner<I, W> wa) {
        this.wa = wa;
    }

    @Override
    public TriggerResult onElement(I element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
        ctx.registerEventTimeTimer(timestamp); //TODO not sure is necessary
        return TriggerResult.CONTINUE;
    }

    public TriggerResult onElement2(I element, long timestamp, W window, TriggerContext ctx) {
        long l = window.maxTimestamp();
        long currentWatermark = ctx.getCurrentWatermark();
        if (l <= currentWatermark) {
            // if the watermark is already past the window fire immediately
            return TriggerResult.FIRE;
        } else {
            ctx.registerEventTimeTimer(l);
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
        Collection<W> ws = wa.assignWindows(null, time, null);
        boolean b = ws.stream()
                .map(TimeWindow::maxTimestamp)
                .anyMatch(max -> max == time);
        return b ? TriggerResult.FIRE : TriggerResult.CONTINUE;
    }

    public ComplexTriggerResult<W> onEventTime(long time) {
        StringBuilder fields = new StringBuilder();
        //TODO: SB Scope Start
        fields.append("find_start_2=").append(System.nanoTime()).append(",");

        Collection<W> ws = wa.assignWindows(null, time, null);

        List<W> toFire = ws.stream().
                filter(w -> w.maxTimestamp() == time)
                .collect(Collectors.toList());

        fields.append("find_end_2=").append(System.nanoTime()).append(",");

        if (toFire.isEmpty()) {
            return new ComplexTriggerResult<>(TriggerResult.CONTINUE, toFire, fields.toString());
        }
//        tctx.registerEventTimeTimer(timestamp); //TODO not sure is necessary
        return new ComplexTriggerResult<>(TriggerResult.FIRE, toFire, fields.toString());
    }

    @Override
    public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }

    public ComplexTriggerResult<W> onWindow(I element, long timestamp, TriggerContext tctx, WindowAssigner.WindowAssignerContext wactx) {

        StringBuilder fields = new StringBuilder();

        //TODO: SB Scope Start
        fields.append("find_start_2=").append(System.nanoTime()).append(",");

        Collection<W> ws = wa.assignWindows(element, timestamp, wactx);

        List<W> toFire = ws.stream().
                filter(w -> TriggerResult.FIRE.equals(onElement2(element, timestamp, w, tctx)))
                .collect(Collectors.toList());

        //TODO: SB Scope End
        fields.append("find_end_2=").append(System.nanoTime()).append(",");

        if (toFire.isEmpty()) {
            tctx.registerEventTimeTimer(timestamp);
            return new ComplexTriggerResult<>(TriggerResult.CONTINUE, toFire, fields.toString());
        }
        return new ComplexTriggerResult<>(TriggerResult.FIRE, toFire, fields.toString());
    }
}

