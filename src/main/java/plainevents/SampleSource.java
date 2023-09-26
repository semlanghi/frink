package plainevents;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import plainevents.datagenerator.DataGenerator;

import java.io.BufferedReader;
import java.io.FileReader;

public class SampleSource implements SourceFunction<SampleEvent> {

    private final long numRecords;
    private final String file;
    private boolean running = true;

    public SampleSource(String file, long numRecords) {
        this.numRecords = numRecords;
        this.file = file;
    }

    @Override
    public void run(SourceContext<SampleEvent> ctx) throws Exception {

        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = reader.readLine();//skip first line;

        int c = 0;
        while (running && line != null && c<numRecords) {
            if(line.length()==0){
                System.err.println("0 chars line");
                line=reader.readLine();
                continue;
            }

            try {
                line = reader.readLine();

                SampleEvent parsed = SampleEvent.parse(line);
                ctx.collectWithTimestamp(parsed, parsed.timestamp());

                c++;
                if(c%1000000==0){
                    System.out.println(c);
                }
            } catch (Exception e){
                System.err.println("exception at " + line);
            }

        }

        reader.close();

    }

    @Override
    public void cancel() {
        running = false;
    }
}
