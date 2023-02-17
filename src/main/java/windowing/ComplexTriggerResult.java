package windowing;

import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import windowing.windows.CandidateTimeWindow;
import windowing.windows.DataDrivenWindow;

import java.util.Collection;

public class ComplexTriggerResult<W> {

    public TriggerResult internalResult;
    public Collection<W> resultWindows;
    public String latencyInfo;

    public ComplexTriggerResult(TriggerResult internalResult, Collection<W> resultWindows, String latencyInfo) {
        this.internalResult = internalResult;
        this.resultWindows = resultWindows;
        this.latencyInfo = latencyInfo;
    }
}
