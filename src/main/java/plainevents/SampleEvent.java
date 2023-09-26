package plainevents;

public class SampleEvent {
    private long key;
    private double value;
    private long ts;

    public SampleEvent(long key, double value, long ts) {
        this.key = key;
        this.value = value;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return ts + "," + key + "," + value;
    }

    public static SampleEvent parse(String s){
        String[] split = s.split(",");
        long ts = Long.parseLong(split[0]);
        long key = Long.parseLong(split[1]);
        double value = Double.parseDouble(split[2]);
        return new SampleEvent(key,value,ts);
    }

    public long timestamp() {
        return ts;
    }

    public double value() {
        return value;
    }

    public long getKey() {
        return key;
    }
}
