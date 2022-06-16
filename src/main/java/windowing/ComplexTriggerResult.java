package windowing;

import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import windowing.windows.CandidateTimeWindow;
import windowing.windows.DataDrivenWindow;

import java.util.Collection;

public class ComplexTriggerResult {

    public TriggerResult internalResult;
    public Collection<DataDrivenWindow> resultWindows;

    public ComplexTriggerResult(TriggerResult internalResult, Collection<DataDrivenWindow> resultWindows) {
        this.internalResult = internalResult;
        this.resultWindows = resultWindows;
    }
}
