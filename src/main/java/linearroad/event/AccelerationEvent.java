package linearroad.event;

public class AccelerationEvent extends RawEvent {
    public AccelerationEvent(){}
    public AccelerationEvent(String k, long ts, double v) {
        super(k, ts, v);
    }
}
