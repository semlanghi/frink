package event;

public class SpeedEvent extends RawEvent {
    public SpeedEvent(){}
    public SpeedEvent(String k, long ts, double v) {
        super(k, ts, v);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SpeedEvent) {
            SpeedEvent other = (SpeedEvent) obj;
            return other.getKey().equals(this.getKey())
                    && other.getValue() == this.getValue() && other.getTimestamp() == this.getTimestamp();
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return 41 * super.hashCode() + Double.hashCode(getValue());
    }



    @Override
    public String toString() {
        return "SpeedEvent(value:"+ getValue() + ", timestamp:" + getTimestamp() + ", key:"+getKey()+")";
    }
}
