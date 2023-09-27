package plainevents.datagenerator;

import plainevents.SampleEvent;

public class DeltaDataGenerator implements DataGenerator {
    private long threshold;
    private long size;
    private long val;
    private long count = 0L;
    private long incrementalTs = 1L;

    public DeltaDataGenerator(long threshold, long size) {
        this.threshold = threshold;
        this.size = size;
        this.val = this.threshold * 5;
    }

    @Override
    public SampleEvent generate() {
        SampleEvent sampleEvent;
        if (count>=size){
            count=0;
            this.val = this.val == threshold * 5 ? threshold * 10 : threshold * 5;
            sampleEvent = new SampleEvent(0L, val, incrementalTs * 1000);
        } else {
            count++;
            sampleEvent = new SampleEvent(0L, val, incrementalTs * 1000);
        }
        incrementalTs++;
        return sampleEvent;
    }
}
