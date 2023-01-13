package linearroad.dataprocessing;

import java.io.*;

/**
 * Class for the pre-processing of the input where we want to insert Out-Of-Order records.
 * The pre-processing consists in adjusting the file format, removing  '[' and ']'.
 */
public class LinearRoadOOOPreProcessing {

    public static void main(String[] args) throws IOException {
        String inputFile = args[0];
        String outputPath = args[1];
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath));

        //skip first line
        String line = reader.readLine();

        line = reader.readLine();
        int c = 0;
        while (line != null) {
            String modifiedLine = line.replace("[",""); //remove starting [
            modifiedLine = modifiedLine.replace("]",""); //remove ending ]
            writer.write(modifiedLine);
            writer.newLine();
            line = reader.readLine();
        }

        writer.flush();
        writer.close();
    }
}