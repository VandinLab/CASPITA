import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Caspita_TOG_MC implements Serializable {

    static Object2IntOpenHashMap<String> patterns;
    static Object2IntOpenHashMap<String> patternsIndex;
    static ObjectOpenHashSet<String> vertices;
    static int[] size;
    static String[] start;
    static Object2ObjectOpenHashMap<String, Pair[]> graphComplete;
    static ObjectArrayList<String> dataset;
    String fileIn;
    String fileOutOver;
    String fileOutUnder;
    JavaSparkContext scc;

    Caspita_TOG_MC(String fileIn, String fileOutOver, String fileOutUnder, JavaSparkContext scc) {
        this.fileIn = fileIn;
        this.fileOutOver = fileOutOver;
        this.fileOutUnder = fileOutUnder;
        this.scc = scc;
    }

    void execute(int P, int T, int par, int k, int h, double FWERTh) {

        if((int)(P*FWERTh)-1<0){
            System.out.println("P = " + P + " Too Low!");
            System.exit(1);
        }

        System.out.println("Loading Paths of Length k = " + k + "...");
        long start = System.currentTimeMillis();
        loadPaths(fileIn, k, h);
        System.out.println("Done in " + (System.currentTimeMillis() - start) + " ms!");

        if (patterns.size() == 0) {
            System.out.println("No Paths of Length k = " + k + " Found!");
            System.exit(1);
        }

        System.out.println("Constructing " + h + "-th Order Generative Null Model...");
        start = System.currentTimeMillis();
        createGraph(h, k);
        System.out.println("Done in " + (System.currentTimeMillis() - start) + " ms!");

        analizeDataset(k,h);
        analyzeGraph();

        System.out.println("Computation P-values Paths...");
        start = System.currentTimeMillis();
        int[][] pValues = computePValues(0, P, T, par, k, h);
        ObjectArrayList<Triple> finalPatterns_over = new ObjectArrayList<>();
        ObjectArrayList<Triple> finalPatterns_under = new ObjectArrayList<>();
        for (String pattern : patterns.keySet()) {
            Triple curr = new Triple(pattern, patterns.getInt(pattern), (1. + pValues[patternsIndex.getInt(pattern)][0]) / ((T / par) * par * 1. + 1.));
            finalPatterns_over.add(curr);
            curr = new Triple(pattern, patterns.getInt(pattern), (1. + pValues[patternsIndex.getInt(pattern)][1]) / ((T / par) * par * 1. + 1.));
            finalPatterns_under.add(curr);
        }
        System.out.println("Done in " + (System.currentTimeMillis() - start) + " ms!");

        Random r = new Random(0);
        double[] minPValues_over = new double[P];
        double[] minPValues_under = new double[P];
        for (int i = 0; i < P; i++) {
            start = System.currentTimeMillis();
            System.out.println("Computation WY " + (i + 1) + "...");
            dataset = generateDatasetRandom(r, h);
            load(k);
            if (patterns.size() == 0) {
                minPValues_over[i] = FWERTh;
                minPValues_under[i] = FWERTh;
            } else {
                pValues = computePValues(i + 1, P, T, par, k, h);
                int min_over = Integer.MAX_VALUE;
                int min_under = Integer.MAX_VALUE;
                for (int j = 0; j < patterns.size(); j++) {
                    if (min_over > pValues[j][0]) min_over = pValues[j][0];
                    if (min_under > pValues[j][1]) min_under = pValues[j][1];
                }
                minPValues_over[i] = Math.min((1. + min_over) / ((T / par) * par * 1. + 1.), FWERTh);
                minPValues_under[i] = Math.min((1. + min_under) / ((T / par) * par * 1. + 1.), FWERTh);
            }
            System.out.println("Done in " + (System.currentTimeMillis() - start) + " ms!");
        }

        Arrays.sort(minPValues_over);
        Arrays.sort(minPValues_under);

        double correctedThreshold_over = minPValues_over[(int) (P * FWERTh) - 1];
        double correctedThreshold_under = minPValues_under[(int) (P * FWERTh) - 1];

        if(correctedThreshold_over == minPValues_over[(int) (P * FWERTh)]){
            int j = (int) (P * FWERTh) - 2;
            while(j > 0 && correctedThreshold_over == minPValues_over[j]) j--;
            correctedThreshold_over = minPValues_over[j];
        }

        if(correctedThreshold_under == minPValues_under[(int) (P * FWERTh)]){
            int j = (int) (P * FWERTh) - 2;
            while(j > 0 && correctedThreshold_under == minPValues_under[j]) j--;
            correctedThreshold_under = minPValues_under[j];
        }

        if (correctedThreshold_over > FWERTh) correctedThreshold_over = FWERTh;
        if (correctedThreshold_under > FWERTh) correctedThreshold_under = FWERTh;
        System.out.println("Corrected Treshold Over Represented Paths: " + correctedThreshold_over);
        System.out.println("Corrected Treshold Under Represented Paths: " + correctedThreshold_under);
        write(correctedThreshold_over, correctedThreshold_under, finalPatterns_over, finalPatterns_under);
    }

    void analyzeGraph() {
        double sum = 0;
        int max = Integer.MIN_VALUE;
        int min = Integer.MAX_VALUE;
        for (String s : graphComplete.keySet()) {
            int curr = graphComplete.get(s).length;
            sum += curr;
            if (max < curr) max = curr;
            if (min > curr) min = curr;

        }
        if (vertices.size() != graphComplete.size()) min = 0;
        System.out.println("AVG Degree: " + (sum) / vertices.size());
        System.out.println("Max Degree: " + max);
        System.out.println("Min Degree: " + min);
        System.out.println("Number of Vertices: " + vertices.size());
        System.out.println("Number of Edges: " + (int)sum);
    }

    void analizeDataset(int k, int h) {
        double avg = 0;
        int tot = 0;
        int totH = 0;
        int n = 0;
        int l = 0;
        for (String s : dataset) {
            String[] split = s.split(" -1 ");
            n++;
            avg += (split.length - 1);
            if (split.length > h) totH += (split.length - h);
            if (split.length > k) tot += (split.length - k);
            if (split.length > l) l = split.length - 1;
        }
        System.out.println("# of Total Paths of Length k = " + k + " in the Real Dataset: " + tot);
        System.out.println("# of Total Patterns of Length h = " + h + " in the Real Dataset: " + totH);
        System.out.println("Number of Transactions of Length > " + h + ": " + n);
        System.out.println("AVG Transactions Length: " + (avg / n));
        System.out.println("Maximum Transactions Length: " + l);
    }

    int[][] computePValues(int iter, int P, int T, int par, int k, int h) {
        IntArrayList indexes = new IntArrayList();
        for (int i = 1; i <= par; i++) indexes.add(i);
        int seed = P * (iter + 1);
        List<int[][]> result;
        result = scc.parallelize(indexes, par).map(o1 -> parallelComputePValues(o1 + seed, T / par, k, h)).collect();
        int[][] pValueInt = new int[result.get(0).length][2];
        for (int[][] curr : result) {
            for (int g = 0; g < curr.length; g++) {
                pValueInt[g][0] += curr[g][0];
                pValueInt[g][1] += curr[g][1];
            }
        }
        return pValueInt;
    }

    private static int[][] parallelComputePValues(int seed, int TforCore, int k, int h) {
        Random r = new Random(seed);
        int numPatt = patterns.size();
        int[][] pValue = new int[numPatt][2];
        for (int j = 0; j < TforCore; j++) {
            Object2IntOpenHashMap<String> pattRandom = generatePatternRandom(r, k, h);
            for (String pattern : pattRandom.keySet()) {
                if (patterns.containsKey(pattern) && pattRandom.getInt(pattern) >= patterns.getInt(pattern)) {
                    pValue[patternsIndex.getInt(pattern)][0]++;
                }
            }
            for (String pattern : patterns.keySet()) {
                if (!pattRandom.containsKey(pattern) || pattRandom.getInt(pattern) <= patterns.getInt(pattern)) {
                    pValue[patternsIndex.getInt(pattern)][1]++;
                }
            }
        }
        return pValue;
    }

    ObjectArrayList<String> generateDatasetRandom(Random r, int h) {
        ObjectArrayList<String> datasetRandom = new ObjectArrayList<>();
        int i = 0;
        while (i < size.length) {
            String first = start[i];
            StringBuilder pattern = new StringBuilder(first);
            int length = h - 1;
            for (int j = h - 1; j < size[i] && graphComplete.containsKey(first); j++) {
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
            if (length == size[i]) {
                datasetRandom.add(pattern.toString());
                i++;
            }
        }
        return datasetRandom;
    }

    private static Object2IntOpenHashMap<String> generatePatternRandom(Random r, int k, int h) {
        Object2IntOpenHashMap<String> pattRandom = new Object2IntOpenHashMap<>();
        int i = 0;
        while (i < size.length) {
            String first = start[i];
            StringBuilder pattern = new StringBuilder(first);
            int length = h - 1;
            for (int j = h - 1; j < size[i] && graphComplete.containsKey(first); j++) {
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
            if (length == size[i]) {
                String[] items = pattern.toString().split(" ");
                StringBuilder pat = new StringBuilder(items[0]);
                for (int j = 1; j < k; j++) pat.append(" ").append(items[j]);
                for (int j = k; j < items.length; j++) {
                    pat.append(" ").append(items[j]);
                    int cont = 1;
                    String p = pat.toString();
                    if (pattRandom.containsKey(p)) cont = pattRandom.removeInt(p) + 1;
                    pattRandom.put(p, cont);
                    int firstIndex = pat.indexOf(" ");
                    pat = new StringBuilder(pat.substring(firstIndex + 1));

                }
                i++;
            }
        }
        return pattRandom;
    }

    void write(double correctedTh_over, double correctedTh_under, ObjectArrayList<Triple> finalPatterns_over, ObjectArrayList<Triple> finalPatterns_under) {
        try {
            ObjectArrayList<Triple> finalPatterns_over_final = new ObjectArrayList<>();
            ObjectArrayList<Triple> finalPatterns_under_final = new ObjectArrayList<>();
            for (Triple p : finalPatterns_over) {
                if (p.pValue < correctedTh_over) finalPatterns_over_final.add(p);
            }
            for (Triple p : finalPatterns_under) {
                if (p.pValue < correctedTh_under) finalPatterns_under_final.add(p);
            }
            System.out.println("Number of Significant Over Represented Paths: " + finalPatterns_over_final.size());
            System.out.println("Number of Significant Under Represented Paths: " + finalPatterns_under_final.size());

            if (finalPatterns_over_final.size() > 0) {
                Collections.sort(finalPatterns_over_final);
                FileWriter fw = new FileWriter(fileOutOver);
                BufferedWriter bw = new BufferedWriter(fw);
                for (Triple t : finalPatterns_over_final) {
                    bw.write(t.patt + " Freq: " + t.freq + " P-value: " + ((t.pValue == 0) ? "~" : "") + t.pValue + "\n");
                }
                bw.close();
                fw.close();
            }
            if (finalPatterns_under_final.size() > 0) {
                Collections.sort(finalPatterns_under_final);
                FileWriter fw = new FileWriter(fileOutUnder);
                BufferedWriter bw = new BufferedWriter(fw);
                for (Triple t : finalPatterns_under_final) {
                    bw.write(t.patt + " Freq: " + t.freq + " P-value: " + ((t.pValue == 0) ? "~" : "") + t.pValue + "\n");
                }
                bw.close();
                fw.close();
            }
        } catch (Exception e) {
            System.err.println("Output File Error!");
            System.exit(1);
        }
    }

    void createGraph(int h, int k) {
        Object2IntOpenHashMap<String> tot = new Object2IntOpenHashMap<>();
        Object2ObjectOpenHashMap<String, Object2IntOpenHashMap<String>> graph = new Object2ObjectOpenHashMap<>();
        vertices = new ObjectOpenHashSet<>();
        int t = 0;
        for (String line : dataset) {
            String[] items = line.split(" -1 ");
            StringBuilder pattern = new StringBuilder(items[0]);
            for (int i = 1; i < h; i++) pattern.append(" ").append(items[i]);
            String p = pattern.toString();
            if (items.length > k) {
                size[t] = items.length - 1;
                start[t++] = p;
            }
            String prev = p;
            int first = pattern.indexOf(" ");
            pattern = new StringBuilder(pattern.substring(first + 1));
            if (first == -1) pattern = new StringBuilder();
            for (int j = h; j < items.length; j++) {
                if (pattern.toString().equals("")) pattern = new StringBuilder(items[j]);
                else pattern.append(" ").append(items[j]);
                p = pattern.toString();
                if (!graph.containsKey(prev)) {
                    vertices.add(prev);
                    Object2IntOpenHashMap<String> currentList = new Object2IntOpenHashMap<>();
                    currentList.put(p, 1);
                    vertices.add(p);
                    graph.put(prev, currentList);
                    tot.put(prev, 1);
                } else {
                    Object2IntOpenHashMap<String> currentList = graph.get(prev);
                    int cont = 1;
                    if (currentList.containsKey(p)) cont = currentList.removeInt(p) + 1;
                    currentList.put(p, cont);
                    vertices.add(p);
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
        System.gc();
    }

    void load(int k) {
        patterns = new Object2IntOpenHashMap<>();
        patternsIndex = new Object2IntOpenHashMap<>();
        int index = 0;
        int tot = 0;
        for (String line : dataset) {
            String[] items = line.split(" ");
            if (items.length > k) {
                StringBuilder pattern = new StringBuilder(items[0]);
                for (int i = 1; i < k; i++) pattern.append(" ").append(items[i]);
                for (int j = k; j < items.length; j++) {
                    pattern.append(" ").append(items[j]);
                    int cont = 1;
                    String p = pattern.toString();
                    if (patterns.containsKey(p)) cont = patterns.removeInt(p) + 1;
                    else patternsIndex.put(p, index++);
                    patterns.put(p, cont);
                    tot++;
                    int first = pattern.indexOf(" ");
                    pattern = new StringBuilder(pattern.substring(first + 1));
                }
            }
        }
        System.out.println("# of Total Paths of Length k = " + k + " in This Random Dataset: " + tot);
        System.out.println("# of Distinct Paths of Length k = " + k + " in This Random Dataset: " + patterns.size());
    }

    void loadPaths(String file, int k, int h) {
        patterns = new Object2IntOpenHashMap<>();
        dataset = new ObjectArrayList<>();
        patternsIndex = new Object2IntOpenHashMap<>();
        try {
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line;
            int index = 0;
            int totTran = 0;
            int tot = 0;
            while ((line = br.readLine()) != null) {
                String sp = line.split("-2")[0];
                String[] items = sp.split(" -1 ");
                if (items.length > h) {
                    dataset.add(sp);
                    if (items.length > k) {
                        totTran++;
                        StringBuilder pattern = new StringBuilder(items[0]);
                        for (int i = 1; i < k; i++) pattern.append(" ").append(items[i]);
                        for (int j = k; j < items.length; j++) {
                            pattern.append(" ").append(items[j]);
                            int cont = 1;
                            String p = pattern.toString();
                            if (patterns.containsKey(p)) cont = patterns.removeInt(p) + 1;
                            else patternsIndex.put(p, index++);
                            patterns.put(p, cont);
                            tot++;
                            int first = pattern.indexOf(" ");
                            pattern = new StringBuilder(pattern.substring(first + 1));
                        }
                    }
                }
            }
            br.close();
            fr.close();
            size = new int[totTran];
            start = new String[totTran];
            System.out.println("# of Total Paths of Length k = " + k + " in the Real Dataset: " + tot);
            System.out.println("# of Distinct Paths of Length k = " + k + " in the Real Dataset: " + patterns.size());
        } catch (IOException e) {
            System.err.println("Input File Error!");
            System.exit(1);
        }
    }

    private static class Pair implements Serializable {
        String s;
        double p;

        Pair(String s, double p) {
            this.s = s;
            this.p = p;
        }
    }

    private static class Triple implements Comparable<Triple>, Serializable {
        String patt;
        int freq;
        double pValue;

        Triple(String patt, int freq, double pValue) {
            this.patt = patt;
            this.freq = freq;
            this.pValue = pValue;
        }

        @Override
        public int compareTo(Triple o) {
            if (pValue > o.pValue) return 1;
            if (pValue < o.pValue) return -1;
            return Integer.compare(o.freq, freq);

        }
    }

    public static void main(String[] args) {
        String dataset = args[0];
        int P = Integer.parseInt(args[1]);
        int M = Integer.parseInt(args[2]);
        int parallelization = Integer.parseInt(args[3]);
        double FWERTh = Double.parseDouble(args[4]);
        String fileIn = "data/" + dataset + ".csv";
        System.out.println("Dataset: " + dataset);
        System.out.println("P: " + P);
        System.out.println("M: " + M);
        System.out.println("Parallelization: " + parallelization);
        System.out.println("FWER Threshold: " + FWERTh);
        Path folder = Paths.get("res/TOG_MC/");
        try {
            Files.createDirectories(folder);
        }
        catch (IOException e){
            System.out.println("Error Creating Folders \"res/TOG_MC/\"!");
            System.exit(1);
        }
        for (int k = 2; k <= 5; k++) {
            for (int h = 1; h < k; h++) {
                System.out.println("k: " + k + " h: " + h);
                String fileOutOver = "res/TOG_MC/" + dataset + "_TOG_MC_" + P + "_" + M + "_" + k + "_" + h + "_OV.txt";
                String fileOutUnder = "res/TOG_MC/" + dataset + "_TOG_MC_" + P + "_" + M + "_" + k + "_" + h + "_UN.txt";
                long start = System.currentTimeMillis();
                SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("Algo").set("spark.executor.memory", "5g")
                        .set("spark.driver.memory", "5g").set("spark.executor.heartbeatInterval", "10000000")
                        .set("spark.network.timeout", "10000000").set("spark.storage.blockManagerSlaveTimeoutMs", "10000000").set("spark.driver.bindAddress", "127.0.0.1");
                JavaSparkContext scc = new JavaSparkContext(sparkConf);
                scc.setLogLevel("ERROR");
                Caspita_TOG_MC caspita = new Caspita_TOG_MC(fileIn, fileOutOver, fileOutUnder, scc);
                caspita.execute(P, M, parallelization, k, h, FWERTh);
                scc.stop();
                System.out.println("Total Execution Time: " + (System.currentTimeMillis() - start) + " ms");
                System.gc();
            }
        }
    }
}