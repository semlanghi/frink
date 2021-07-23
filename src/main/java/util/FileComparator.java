package util;

import java.io.*;

public class FileComparator {

    public static void main(String[] args) throws IOException {
        String line1;
        String line2;
        boolean found = false;
        FileReader fileReader = new FileReader(new File("/Users/samuelelanghi/Documents/projects/ICEP/ICEP/output-frame_multi_delta"));

        BufferedReader bufferedReader1 = new BufferedReader(fileReader);

        File file;
        FileWriter fileWriter = new FileWriter(new File("/Users/samuelelanghi/Documents/projects/ICEP/ICEP/diff.txt"));

        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(new File("/Users/samuelelanghi/Documents/projects/ICEP/ICEP/output-frame_single_delta")))) {
            String line, line3;

            while ((line = bufferedReader.readLine()) != null ) {
                // ... do something with line
                found =false;

                bufferedReader1.close();
                fileReader = new FileReader(new File("/Users/samuelelanghi/Documents/projects/ICEP/ICEP/output-frame_multi_delta"));

                bufferedReader1 = new BufferedReader(fileReader);

                while((line3 = bufferedReader1.readLine()) != null && !found){

                    if(line.equals(line3)){
                        found = true;
                    }


                }


                if(!found){
                    fileWriter.write(line+"\n");
                    fileWriter.flush();
                }

            }

            bufferedReader.close();
            bufferedReader1.close();
        } catch (IOException e) {
            // ... handle IO exception
        }
    }
}
