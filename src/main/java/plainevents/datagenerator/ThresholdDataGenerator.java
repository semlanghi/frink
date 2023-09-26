package plainevents.datagenerator;

import plainevents.SampleEvent;

public class ThresholdDataGenerator implements DataGenerator {
    private long threshold;
    private long size;
    private long count = 0L;
    private long incrementalTs = 1L;

    public ThresholdDataGenerator(long threshold, long size) {
        this.threshold = threshold;
        this.size = size;
    }

    @Override
    public SampleEvent generate() {
        SampleEvent sampleEvent;
        if (count>=size){
            count=0;
            sampleEvent = new SampleEvent(0L, threshold / 2.0d, incrementalTs * 1000);
        } else {
            count++;
            sampleEvent = new SampleEvent(0L, threshold * 2.0d, incrementalTs * 1000);
        }
        incrementalTs++;
        return sampleEvent;
    }
}
