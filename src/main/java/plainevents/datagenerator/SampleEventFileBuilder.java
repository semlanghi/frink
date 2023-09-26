package plainevents.datagenerator;

import linearroad.event.SpeedEvent;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.*;
import java.util.Arrays;
import java.util.function.ToLongFunction;

public class SampleEventFileBuilder {

    public static void main(String[] args) throws IOException {
        ParameterTool parameters = ParameterTool.fromArgs(args);

        long numRecordsToEmit = parameters.getLong("max-events");
        long recordsEmitted = 0L;
        String windowType = parameters.getRequired("windowType");
        long[] windowParams = Arrays.stream(parameters
                        .getRequired("windowParams")
                        .split(";"))
                        .mapToLong(Long::parseLong)
                        .toArray();
        String genFileLocation = parameters.getRequired("path");


        DataGenerator dataGenerator = DataGenerator.getFrameDatGenerator(windowType, windowParams);

        BufferedWriter writer = new BufferedWriter(new FileWriter(genFileLocation + "sample-" +
                parameters.getRequired("windowType") + "-" + parameters.getRequired("windowParams") +".csv"));
        writer.write("ts,key,value");
        writer.newLine();


        while (recordsEmitted <= numRecordsToEmit){
            String eventStr = dataGenerator.generate().toString();
            writer.write(eventStr);
            writer.newLine();

            if(recordsEmitted%10000000 == 0){
                System.out.println(eventStr);
                writer.flush();
            }

            recordsEmitted++;
        }

        writer.flush();

        writer.close();


    }
}
