package linearroad.dataprocessing;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public class LinearRoadOOOPostProcessing {

    public static void main(String[] args) throws IOException {
        String file = args[0];

        BufferedReader reader = new BufferedReader(new FileReader(file));
        BufferedWriter writer = new BufferedWriter(new FileWriter(args[1]));
        String line = reader.readLine();//skip first line;
        writer.write("VID,SPEED,ACCEL,XWay,Lane,Dir,Seg,Pos,T1,T2");
        writer.newLine();

        int c = 0;
        while (line != null) {
            if(line.length()==0){
                System.err.println("0 chars line");
                line=reader.readLine();
                continue;
            }
            line = line.substring(0, line.length()-1);
            String[] lineParts = line.split(","); //remove starting [

            try {
                writer.write(toLinearRoadOriginalRow(lineParts));
                writer.newLine();
                line = reader.readLine();
                c++;
                if(c%1000000==0){
                    System.out.println(c);
                }
            } catch (Exception e){
                System.err.println(line);
            }

        }

        writer.flush();

    }

    private static String toLinearRoadOriginalRow(String[] elements){
        if(elements.length!=12){
            System.out.println(elements);
            throw new IllegalArgumentException("Expected 12 elements in array");
        }

        List<String> elementList = new ArrayList<>(Arrays.asList(elements));

        elementList.set(10,elementList.get(0)); //set new event time  in old event time field;
        elementList.remove(0); //remove first two elements (added by out of order)
        elementList.remove(0);

        StringBuffer sb = new StringBuffer();
        sb.append("[");
        sb.append(elementList.stream().collect(Collectors.joining(",")));
        sb.append("]");

        return sb.toString();
    }
}
