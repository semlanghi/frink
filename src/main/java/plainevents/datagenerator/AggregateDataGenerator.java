package plainevents.datagenerator;

import plainevents.SampleEvent;

public class AggregateDataGenerator implements DataGenerator {
    private long threshold;
    private long size;
    private long increment;
    private long count = 0L;
    private long incrementalTs = 1L;

    public AggregateDataGenerator(long threshold, long size) {
        this.threshold = threshold;
        this.size = size;
        this.increment = threshold/size  == 0 ? 1 : threshold/size;
    }

    @Override
    public SampleEvent generate() {

        SampleEvent sampleEvent = new SampleEvent(0L, this.increment, incrementalTs * 1000);
        incrementalTs++;

        return sampleEvent;
    }
}
