import java.io.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;

public class Bike_10_20 {

    static ArrayList<String> dataset = new ArrayList<>();
    static HashMap<String, Integer> count = new HashMap<>();
    static ArrayList<Pair> ordered = new ArrayList<>();

    static class Pair implements Comparable<Pair> {
        String s;
        Integer c;

        Pair(String s, int c) {
            this.s = s;
            this.c = c;
        }

        @Override
        public int compareTo(Pair o) {
            return -c.compareTo(o.c);
        }
    }

    static void read(String file) {
        try {
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line;
            while ((line = br.readLine()) != null) {
                dataset.add(line);
                String[] split = line.split("-2")[0].split(" -1 ");
                for (String s : split) {
                    int cont = 1;
                    if (count.containsKey(s)) cont = count.remove(s) + 1;
                    count.put(s, cont);
                }
            }
            br.close();
            fr.close();
            for (String s : count.keySet()) {
                ordered.add(new Pair(s, count.get(s)));
            }
            ordered.sort(Comparator.naturalOrder());
        } catch (IOException e) {
            System.out.println("Error Reading BIKE!");
            System.exit(1);
        }
    }

    static void write(String file, int n){
        try {
            HashSet<String> best = new HashSet<>();
            for (int i = 0; i < n; i++) best.add(ordered.get(i).s);
            FileWriter fw = new FileWriter(file);
            BufferedWriter bw = new BufferedWriter(fw);
            for (String s : dataset) {
                String[] split = s.split("-2")[0].split(" -1 ");
                boolean ok = true;
                for (int i = 0; i < split.length && ok; i++) {
                    ok = best.contains(split[i]);
                }
                if (ok) bw.write(s + "\n");
            }
            bw.close();
            fw.close();
        } catch (IOException e) {
            System.out.println("Error Writing " + file + "!");
            System.exit(1);
        }
    }

    public static void main(String[] args){
        read("data/2019_BIKE.csv");
        int n = Integer.parseInt(args[0]);
        if (n != 10 && n != 20) System.exit(1);
        String fileOut = "data/2019_BIKE_" + n + ".csv";
        write(fileOut, n);
    }
}
