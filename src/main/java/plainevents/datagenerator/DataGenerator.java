package plainevents.datagenerator;

import plainevents.SampleEvent;

public interface DataGenerator {
    public static DataGenerator getFrameDatGenerator(String frameDescription, long... args){
        switch (frameDescription){
            case "threshold": return new ThresholdDataGenerator(args[0], args[1]);
            case "delta": return new DeltaDataGenerator(args[0], args[1]);
            case "aggregate": return new AggregateDataGenerator(args[0], args[1]);
        }
        throw new UnsupportedOperationException("Window" + frameDescription + " not supported.");
    }
    public SampleEvent generate();

}
