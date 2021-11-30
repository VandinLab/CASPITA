import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.io.*;


public class Bike {

    private static HashMap<Integer, ArrayList<Short>> dataset = new HashMap<>();

    public static void loadData(String file) {
        try {
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            br.readLine();
            String line = br.readLine();
            while (line != null) {
                String[] splitLine = line.split(",");
                try {
                    int bike = Integer.parseInt(splitLine[10].replace("\"", ""));
                    short start = Short.parseShort(splitLine[4]);
                    short end = Short.parseShort(splitLine[7]);
                    ArrayList<Short> ride;
                    if (dataset.containsKey(bike)) ride = dataset.remove(bike);
                    else ride = new ArrayList<>();
                    ride.add(start);
                    ride.add(end);
                    dataset.put(bike, ride);
                } catch (NumberFormatException e) {
                    System.err.println("Value not valid: " + splitLine[10] + " " + splitLine[4] + " " + splitLine[7]);
                }
                line = br.readLine();
            }
            br.close();
            fr.close();
        } catch (IOException e) {
            System.out.println("Error Reading " + file + "!");
            System.exit(1);
        }
    }

    public static void writeData(String file){
        try{
            FileWriter fw = new FileWriter(file);
            BufferedWriter bw = new BufferedWriter(fw);

            for(ArrayList<Short> ride:dataset.values()){
                bw.write(ride.get(0) + " -1 ");
                for(int i = 1;i<ride.size()-1;i+=2){
                    short end = ride.get(i);
                    short start = ride.get(i+1);
                    bw.write(end + " -1 ");
                    if(start!=end) bw.write("-2\n" + start + " -1 ");
                }
                bw.write(ride.get(ride.size()-1) + " -1 ");
                bw.write("-2\n");
            }
            bw.close();
            fw.close();
        } catch (IOException e) {
            System.out.println("Error Writing " + file + "!");
            System.exit(1);
        }
    }

    public static void main(String[] args){
        int year = Integer.parseInt(args[0]);
        if(year!=2019 && year!=2020) System.exit(1);
        String file = "original/"+year+"q1.csv";
        String file2 = "original/"+year+"q2.csv";
        String file3 = "original/"+year+"q3.csv";
        String file4 = "original/"+year+"q4.csv";
        loadData(file);
        loadData(file2);
        loadData(file3);
        loadData(file4);
        Path folder = Paths.get("data/");
        try {
            Files.createDirectories(folder);
        }
        catch (IOException e){
            System.out.println("Error Creating Folder \"data\"!");
            System.exit(1);
        }
        String fileOut = "data/"+year+"_BIKE.csv";
        writeData(fileOut);
    }
}