import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ArtificialTestHYPA {

    String fileIn;

    ArtificialTestHYPA(String fileIn){
        this.fileIn = fileIn;
    }

    int[] execute(double threshold){
        int[] res = new int[2];
        try {
            FileReader fr = new FileReader(fileIn);
            BufferedReader br = new BufferedReader(fr);
            String line;
            while ((line = br.readLine()) != null) {
                double val = Double.parseDouble(line.split(" hypa score: ")[1]);
                if(val >= 1 - threshold) res[0]++; //Over
                if(val < threshold) res[1]++; //Under
            }
            br.close();
            fr.close();
        } catch (IOException e) {
            System.err.println("Input File Error!");
            System.exit(1);
        }
        return res;
    }

    public static void main(String[] args){
        String dataset = args[0];
        double threshold = Double.parseDouble(args[1]);
        System.out.println("Dataset: " + dataset);
        System.out.println("Threshold: " + threshold );
        Path folder = Paths.get("res/HYPA/");
        try {
            Files.createDirectories(folder);
        }
        catch (IOException e){
            System.out.println("Error Creating Folders \"res/HYPA/\"!");
            System.exit(1);
        }
        int numTest = 20;
        double tot = 0;
        for(int i = 0; i < numTest; i++) {
            for (int k = 2; k <= 5; k++) {
                int seed = 100+i+1;
                    String fileInART = "res/HYPA/" + dataset + "_" + seed + "_" + k + "_" + (k-1) + "_HYPA_OUT.txt";
                    ArtificialTestHYPA AT = new ArtificialTestHYPA(fileInART);
                    int[] res = AT.execute(threshold);
                    int numOver = res[0];
                    int numUnder = res[1];
                    if(numOver>0) tot++;
                    if(numUnder>0) tot++;
            }
        }
        System.out.println("FWER: " + tot/(numTest*8));
    }
}