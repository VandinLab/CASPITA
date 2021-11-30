import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

public class ArtificialTest {

    private class Pair {
        String s;
        double p;

        Pair(String s, double p) {
            this.s = s;
            this.p = p;
        }
    }

    static Object2IntOpenHashMap<String> starts;
    static Object2ObjectOpenHashMap<String, Pair[]> graphComplete;
    static ObjectArrayList<String> dataset;
    static Object2IntOpenHashMap<String> patterns;
    String fileIn;
    String fileOut;


    ArtificialTest(String fileIn, String fileOut){
        this.fileIn = fileIn;
        this.fileOut = fileOut;
    }

    void execute(int h, int k, int sedd){
        loadStart(k,h);
        createGraph(h);
        Random r = new Random(sedd);
        generatePathsRandom(r,k,h);
        write();
    }

    private void generatePathsRandom(Random r, int k, int h) {
        patterns = new Object2IntOpenHashMap<>();
        for (String s : starts.keySet()) {
            int i = 0;
            while (i < starts.getInt(s)) {
                StringBuilder pattern = new StringBuilder(s);
                String first = pattern.toString();
                int length = h - 1;
                for (int j = h - 1; j < k && graphComplete.containsKey(first); j++) {
                    double val = r.nextDouble();
                    int g = 0;
                    Pair[] pp = graphComplete.get(first);
                    for (Pair p : pp) {
                        if (val < p.p) break;
                        g++;
                    }
                    String second = pp[g].s;
                    String[] last = second.split(" ");
                    pattern.append(" ").append(last[last.length - 1]);
                    first = second;
                    length++;
                }
                if (length == k) {
                    String p = pattern.toString();
                    int cont = 1;
                    if (patterns.containsKey(p)) cont = patterns.removeInt(p) + 1;
                    patterns.put(p, cont);
                    i++;
                }
            }
        }
    }

    void createGraph(int h) {
        Object2IntOpenHashMap<String> tot = new Object2IntOpenHashMap<>();
        Object2ObjectOpenHashMap<String, Object2IntOpenHashMap<String>> graph = new Object2ObjectOpenHashMap<>();
        for (String line : dataset) {
            String[] items = line.split(" -1 ");
            StringBuilder pattern = new StringBuilder(items[0]);
            for (int i = 1; i < h; i++) pattern.append(" ").append(items[i]);
            String p = pattern.toString();
            String prev = p;
            int first = pattern.indexOf(" ");
            pattern = new StringBuilder(pattern.substring(first + 1));
            if (first == -1) pattern = new StringBuilder();
            for (int j = h; j < items.length; j++) {
                if (pattern.toString().equals("")) pattern = new StringBuilder(items[j]);
                else pattern.append(" ").append(items[j]);
                p = pattern.toString();
                if (!graph.containsKey(prev)) {
                    Object2IntOpenHashMap<String> currentList = new Object2IntOpenHashMap<>();
                    currentList.put(p, 1);
                    graph.put(prev, currentList);
                    tot.put(prev, 1);
                } else {
                    Object2IntOpenHashMap<String> currentList = graph.get(prev);
                    int cont = 1;
                    if (currentList.containsKey(p)) cont = currentList.removeInt(p) + 1;
                    currentList.put(p, cont);
                    cont = tot.removeInt(prev) + 1;
                    tot.put(prev, cont);
                }
                prev = p;
                first = pattern.indexOf(" ");
                pattern = new StringBuilder(pattern.substring(first + 1));
                if (first == -1) pattern = new StringBuilder();
            }
        }
        graphComplete = new Object2ObjectOpenHashMap<>();
        for (String s : graph.keySet()) {
            Object2IntOpenHashMap<String> current = graph.get(s);
            Pair[] p = new Pair[current.size()];
            int i = 0;
            int curr_tot = 0;
            int tt = tot.getInt(s);
            for (String ss : current.keySet()) {
                curr_tot += current.getInt(ss);
                p[i++] = new Pair(ss, curr_tot / (tt * 1.));
            }
            graphComplete.put(s, p);

        }
        graph = null;
        tot = null;
        dataset = null;
        System.gc();
    }

    void loadStart(int k, int h) {
        starts = new Object2IntOpenHashMap<>();
        dataset = new ObjectArrayList<>();
        try {
            FileReader fr = new FileReader(fileIn);
            BufferedReader br = new BufferedReader(fr);
            String line;
            while ((line = br.readLine()) != null) {
                String sp = line.split("-2")[0];
                String[] items = sp.split(" -1 ");
                if (items.length > h) {
                    dataset.add(sp);
                    if (items.length > k) {
                        StringBuilder pattern = new StringBuilder(items[0]);
                        for (int i = 1; i < h; i++) pattern.append(" ").append(items[i]);
                        int cont = 1;
                        String p = pattern.toString();
                        if (starts.containsKey(p)) cont = starts.removeInt(p) + 1;
                        starts.put(p, cont);
                        int s = 1;
                        for (int j = h; items.length - s > k; j++) {
                            int first = pattern.indexOf(" ");
                            pattern = new StringBuilder(pattern.substring(first + 1));
                            if (first == -1) pattern = new StringBuilder(items[j]);
                            else pattern.append(" ").append(items[j]);
                            p = pattern.toString();
                            cont = 1;
                            if (starts.containsKey(p)) cont = starts.removeInt(p) + 1;
                            starts.put(p, cont);
                            s++;
                        }
                    }
                }
            }
            br.close();
            fr.close();
        } catch (IOException e) {
            System.err.println("Input File Error!");
            System.exit(1);
        }
    }

    void write() {
        try {
            FileWriter fw = new FileWriter(fileOut);
            BufferedWriter bw = new BufferedWriter(fw);
            for(String p : patterns.keySet()){
                int n = patterns.getInt(p);
                String s = p.replace(" "," -1 ") + " -1 -2\n";
                for(int i = 0; i < n; i++ ){
                    bw.write(s);
                }
            }
            bw.close();
            fw.close();
        } catch (IOException e) {
            System.err.println("Output File Error!");
            System.exit(1);
        }
    }

    public static void main(String[] args){
        String dataset = args[0];
        int P = Integer.parseInt(args[1]);
        double FWERTh = 0.05;
        String fileIn = "data/" + dataset + ".csv";
        System.out.println("Dataset: " + dataset);
        PrintStream originalStream = System.out;
        int numTest = 20;
        double tot = 0;
        Path folder = Paths.get("res/Artificial/");
        try {
            Files.createDirectories(folder);
        }
        catch (IOException e){
            System.out.println("Error Creating Folders \"res/Artificial/\"!");
            System.exit(1);
        }
        for(int i = 0; i < numTest; i++) {
            for (int k = 2; k <= 5; k++) {
                for (int h = 1; h < k; h++) {
                    int seed = P+i+1;
                    System.out.println("k: " + k + " h: " + h);
                    long start = System.currentTimeMillis();
                    String fileInART =   dataset + "_" + seed + "_" + k + "_" + h + ".csv";
                    ArtificialTest AT = new ArtificialTest(fileIn, fileInART);
                    AT.execute(h, k, seed);
                    String fileOutOver = "res/Artificial/" + dataset + "_" + seed + "_" + P + "_" + k + "_" + h + "_OV.txt";
                    String fileOutUnder = "res/Artificial/" + dataset + "_" + seed + "_" + P + "_" + k + "_" + h + "_UN.txt";
                    Caspita_POG_Bin caspita = new Caspita_POG_Bin(fileInART, fileOutOver, fileOutUnder, true);
                    Object[] res = caspita.execute(P, k, h, FWERTh);
                    System.setOut(originalStream);
                    System.out.println("Total Execution Time: " + (System.currentTimeMillis() - start) + " ms");
                    int numOver = (int)res[1];
                    int numUnder = (int)res[3];
                    if(numOver>0) tot++;
                    if(numUnder>0) tot++;
                    File f = new File(fileInART);
                    f.delete();
                    System.gc();
                }
            }
        }
        System.out.println("FWER: " + tot/(numTest*20));
    }
}