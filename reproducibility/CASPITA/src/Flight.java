import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


public class Flight {

    static void convert(String fileIn, String fileOut){
        Long2ObjectOpenHashMap<Pair[]> data = new Long2ObjectOpenHashMap<>();
        try {
            FileReader fr = new FileReader(fileIn);
            BufferedReader br = new BufferedReader(fr);
            String line;
            line = br.readLine();
            while ((line = br.readLine()) != null) {
                String[] splitted = line.split(",");
                long it = Long.parseLong(splitted[0]);
                int seq = Integer.parseInt(splitted[1]);
                int start = Integer.parseInt(splitted[2]);
                int end = Integer.parseInt(splitted[3]);
                Pair[] curr;
                if (data.containsKey(it)) {
                    curr = data.remove(it);
                    if (seq > curr.length) {
                        Pair[] newCurr = new Pair[seq];
                        for (int i = 0; i < curr.length; i++) newCurr[i] = curr[i];
                        newCurr[seq - 1] = new Pair(start, end);
                        data.put(it, newCurr);
                    } else {
                        curr[seq - 1] = new Pair(start, end);
                        data.put(it, curr);
                    }
                } else {
                    curr = new Pair[seq];
                    curr[seq - 1] = new Pair(start, end);
                    data.put(it, curr);
                }
            }
            br.close();
            fr.close();
        }
        catch(IOException e){
            System.out.println("Error Reading " + fileIn + "!");
            System.exit(1);
        }
        try{
            FileWriter fw = new FileWriter(fileOut, true);
            BufferedWriter bw = new BufferedWriter(fw);
            for (long i : data.keySet()) {
                Pair[] curr = data.get(i);
                StringBuilder sb = new StringBuilder(curr[0].s + " -1 ");
                boolean ok = true;
                if (curr.length == 1) bw.write(sb.toString() + curr[0].e + " -1 -2\n");
                else {
                    for (int j = 1; j < curr.length && ok; j++) {
                        if (curr[j - 1].e == curr[j].s && curr[j].s != 0) {
                            sb.append(curr[j].e).append(" -1 ");
                        } else ok = false;

                    }
                    if (ok) bw.write(sb + "-2\n");
                }
            }
            bw.close();
            fw.close();
        }
        catch(IOException e){
            System.out.println("Error Writing " + fileOut + "!");
            System.exit(1);
        }
    }

    private static class Pair {
        int s;
        int e;

        Pair(int s, int e) {
            this.s = s;
            this.e = e;
        }
    }

    public static void main(String[] args){
        Path folder = Paths.get("data/");
        try {
            Files.createDirectories(folder);
        }
        catch (IOException e){
            System.out.println("Error Creating Folder \"data\"!");
            System.exit(1);
        }
        convert("original/mark_q1_2019_C.csv", "data/2019_flight.csv");
        convert("original/mark_q2_2019_C.csv", "data/2019_flight.csv");
        convert("original/mark_q3_2019_C.csv", "data/2019_flight.csv");
        convert("original/mark_q4_2019_C.csv", "data/2019_flight.csv");
    }
}