import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.*;
import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;


public class TestPValues implements Serializable {

    static Object2IntOpenHashMap<String> patterns;
    static Object2IntOpenHashMap<String> patternsIndex;
    static Object2IntOpenHashMap<String> starts;
    static Object2DoubleOpenHashMap<String> prob;
    static Object2ObjectOpenHashMap<String, Pair[]> graphComplete;
    static Object2ObjectOpenHashMap<String, Object2DoubleOpenHashMap<String>> graphComplete2;
    static Object2ObjectOpenHashMap<String, Object2ObjectOpenHashMap<String, Object2DoubleOpenHashMap<String>>> modEdges;
    static ObjectArrayList<String> dataset;
    static ObjectOpenHashSet<String> vertices;
    JavaSparkContext scc;
    String fileIn;
    String fileOutOver;
    String fileOutUnder;
    int seed;


    TestPValues(String fileIn, String fileOutOver, String fileOutUnder, JavaSparkContext scc, int seed) {
        this.fileIn = fileIn;
        this.fileOutOver = fileOutOver;
        this.fileOutUnder = fileOutUnder;
        this.scc = scc;
        this.seed = seed;
    }


    void execute(int T, int par, int k, int h) {

        loadPaths(fileIn, k, h);
        if (patterns.size() == 0) {
            System.out.println("No patterns of length " + k + " found!");
            System.exit(1);
        }
        createGraph(h);
        loadStart(k, h);
        computeProbability(h);
        if (vertices.size() != graphComplete.size() && k != h) {
            computeModEdges(k, h);
        }

        long start = System.currentTimeMillis();
        System.out.println("Computation p-values Binomial...");
        Object2DoubleOpenHashMap<String>[] pv_bin = computePValuesBin(h);
        ObjectArrayList<Triple> finalPatterns_over = new ObjectArrayList<>();
        ObjectArrayList<Triple> finalPatterns_under = new ObjectArrayList<>();
        for (String pattern : patterns.keySet()) {
            Triple curr = new Triple(pattern, patterns.getInt(pattern), pv_bin[0].getDouble(pattern));
            finalPatterns_over.add(curr);
            curr = new Triple(pattern, patterns.getInt(pattern), pv_bin[1].getDouble(pattern));
            finalPatterns_under.add(curr);
        }
        Collections.sort(finalPatterns_over);
        Collections.sort(finalPatterns_under);
        System.out.println("Done in " + (System.currentTimeMillis() - start) + " ms!");

        start = System.currentTimeMillis();
        System.out.println("Computation p-values MC with T = " + T + ": ");
        int[][] pValues = computePValuesMC(T, par, k, h);
        Object2DoubleOpenHashMap<String> finalPatterns_over_MC = new Object2DoubleOpenHashMap<>();
        Object2DoubleOpenHashMap<String> finalPatterns_under_MC = new Object2DoubleOpenHashMap<>();
        for (String pattern : patterns.keySet()) {
            finalPatterns_over_MC.put(pattern, (1. + pValues[patternsIndex.getInt(pattern)][0]) / ((T / par) * par * 1. + 1.));
            finalPatterns_under_MC.put(pattern, (1. + pValues[patternsIndex.getInt(pattern)][1]) / ((T / par) * par * 1. + 1.));
        }
        System.out.println("Done in " + (System.currentTimeMillis() - start) + " ms!");

        write(finalPatterns_over, finalPatterns_over_MC, finalPatterns_under, finalPatterns_under_MC);
    }

    void computeModEdges(int k, int h) {
        modEdges = new Object2ObjectOpenHashMap<>();
        for (String start : starts.keySet()) {
            Object2ObjectOpenHashMap<String, Object2DoubleOpenHashMap<String>> currMap = new Object2ObjectOpenHashMap();
            ObjectArrayList<String>[] stack = new ObjectArrayList[k - h + 1];
            ObjectOpenHashSet<String>[] remove = new ObjectOpenHashSet[k - h];
            double[] sum = new double[k - h];
            stack[0] = new ObjectArrayList<>();
            for (int i = 0; i < k - h; i++) {
                stack[i + 1] = new ObjectArrayList<>();
                remove[i] = new ObjectOpenHashSet<>();
            }
            Object2DoubleOpenHashMap<String> current = graphComplete2.get(start);
            int level = 1;
            stack[0].add(start);
            for (String s : current.keySet()) stack[1].add(s);
            while (level > 0) {
                if (stack[level].isEmpty()) {
                    level--;
                    if (remove[level].size() > 0) {
                        String prev = stack[level].get(stack[level].size() - 1);
                        if (remove[level].size() == graphComplete2.get(prev).size()) {
                            remove[level - 1].add(prev);
                            sum[level - 1] += graphComplete2.get(stack[level - 1].get(stack[level - 1].size() - 1)).getDouble(prev);
                        } else {
                            Object2DoubleOpenHashMap<String> c = graphComplete2.get(prev);
                            sum[level] = 1. - sum[level];
                            Object2DoubleOpenHashMap<String> curr;
                            if (currMap.containsKey(prev)) curr = currMap.get(prev);
                            else curr = new Object2DoubleOpenHashMap<>();
                            for (String s : c.keySet()) {
                                if (!remove[level].contains(s)) curr.put(s + ":" + level, c.getDouble(s) / sum[level]);
                            }
                            currMap.put(prev, curr);
                        }
                    }
                    remove[level] = new ObjectOpenHashSet<>();
                    sum[level] = 0;
                    stack[level].remove(stack[level].size() - 1);
                } else {
                    String curVert = stack[level].get(stack[level].size() - 1);
                    if (!graphComplete2.containsKey(curVert)) {
                        if (level < k - h + 1) {
                            remove[level - 1].add(curVert);
                            sum[level - 1] += graphComplete2.get(stack[level - 1].get(stack[level - 1].size() - 1)).getDouble(curVert);
                        }
                        stack[level].remove(stack[level].size() - 1);
                    } else {
                        if (level < k - h) {
                            current = graphComplete2.get(curVert);
                            level++;
                            for (String s : current.keySet()) stack[level].add(s);
                        } else stack[level].remove(stack[level].size() - 1);
                    }
                }
            }
            if (currMap.size() > 0) modEdges.put(start, currMap);
        }
    }

    void write(ObjectArrayList<Triple> finalPatterns_o, Object2DoubleOpenHashMap<String> finalPatterns_o_MC,
                   ObjectArrayList<Triple> finalPatterns_u, Object2DoubleOpenHashMap<String> finalPatterns_u_MC) {
        try {
            FileWriter fw = new FileWriter(fileOutOver);
            BufferedWriter bw = new BufferedWriter(fw);
            for (Triple t : finalPatterns_o) {
                double pv_MC = finalPatterns_o_MC.getDouble(t.patt);
                bw.write(t.patt + " Freq: " + t.freq + " P-value_Bin: " + t.pValue + " P-value_MC" + ": " + pv_MC + "\n");
            }
            bw.close();
            fw.close();
        } catch (IOException e) {
            System.err.println("Output File Error!");
            System.exit(1);
        }
        try {
            FileWriter fw = new FileWriter(fileOutUnder);
            BufferedWriter bw = new BufferedWriter(fw);
            for (Triple t : finalPatterns_u) {
                double pv_MC = finalPatterns_u_MC.getDouble(t.patt);
                bw.write(t.patt + " Freq: " + t.freq + " P-value_Bin: " + t.pValue + " P-value_MC" + ": " + pv_MC + "\n");
            }
            bw.close();
            fw.close();
        } catch (IOException e) {
            System.err.println("Output File Error!");
            System.exit(1);
        }
    }

    private static Object2IntOpenHashMap<String> generatePathsRandom(Random r, int k, int h) {
        Object2IntOpenHashMap<String> pattRandom = new Object2IntOpenHashMap<>();
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
                    if (pattRandom.containsKey(p)) cont = pattRandom.removeInt(p) + 1;
                    pattRandom.put(p, cont);
                    i++;
                }
            }
        }
        return pattRandom;
    }

    int[][] computePValuesMC(int T, int par, int k, int h) {
        IntArrayList indexes = new IntArrayList();
        for (int i = 1; i <= par; i++) indexes.add(i);
        int s = seed;
        List<int[][]> result;
        result = scc.parallelize(indexes, par).map(o1 -> parallelComputePValues(o1 + s, T / par, k, h)).collect();
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
            Object2IntOpenHashMap<String> pattRandom;
            pattRandom = generatePathsRandom(r, k, h);
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

    void computeProbability(int h) {
        for (String s : patterns.keySet()) {
            if (!prob.containsKey(s)) {
                double p = 1.;
                String[] ss = s.split(" ");
                StringBuilder prev = new StringBuilder(ss[0]);
                for (int i = 1; i < h; i++) prev.append(" ").append(ss[i]);
                String pr = prev.toString();
                int first = prev.indexOf(" ");
                StringBuilder pattern = new StringBuilder(prev.substring(first + 1)).append(" ");
                if (first == -1) pattern = new StringBuilder();
                if (modEdges == null || !modEdges.containsKey(pr)) {
                    for (int j = h; j < ss.length; j++) {
                        pattern.append(ss[j]);
                        String pat = pattern.toString();
                        p *= graphComplete2.get(pr).getDouble(pat);
                        prev = pattern;
                        pr = prev.toString();
                        first = prev.indexOf(" ");
                        pattern = new StringBuilder(prev.substring(first + 1)).append(" ");
                        if (first == -1) pattern = new StringBuilder();
                    }
                } else {
                    Object2ObjectOpenHashMap<String, Object2DoubleOpenHashMap<String>> current = modEdges.get(pr);
                    int level = 0;
                    for (int j = h; j < ss.length; j++) {
                        pattern.append(ss[j]);
                        String pat = pattern.toString();
                        if (current.containsKey(pr) && current.get(pr).containsKey(pat + ":" + level)) {
                            p *= current.get(pr).getDouble(pat + ":" + level);
                        } else p *= graphComplete2.get(pr).getDouble(pat);
                        prev = pattern;
                        pr = prev.toString();
                        first = prev.indexOf(" ");
                        pattern = new StringBuilder(prev.substring(first + 1)).append(" ");
                        if (first == -1) pattern = new StringBuilder();
                        level++;
                    }

                }
                prob.put(s, Math.min(p, 1.));
            }
        }
    }

    Object2DoubleOpenHashMap<String>[] computePValuesBin(int h) {
        Object2DoubleOpenHashMap<String>[] pValues = new Object2DoubleOpenHashMap[2];
        pValues[0] = new Object2DoubleOpenHashMap<>(); //Over
        pValues[1] = new Object2DoubleOpenHashMap<>(); //Under
        for (String s : patterns.keySet()) {
            String[] ss = s.split(" ");
            StringBuilder prev = new StringBuilder(ss[0]);
            for (int i = 1; i < h; i++) prev.append(" ").append(ss[i]);
            double p = prob.getDouble(s);
            int n = starts.getInt(prev.toString());
            int f = patterns.getInt(s);
            BinomialDistribution b = new BinomialDistribution(n, p);
            pValues[0].put(s, 1 - b.cumulativeProbability(f - 1));
            pValues[1].put(s, b.cumulativeProbability(f));
        }
        return pValues;
    }

    void createGraph(int h) {
        Object2IntOpenHashMap<String> tot = new Object2IntOpenHashMap<>();
        Object2ObjectOpenHashMap<String, Object2IntOpenHashMap<String>> graph = new Object2ObjectOpenHashMap<>();
        vertices = new ObjectOpenHashSet<>();
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
        graphComplete2 = new Object2ObjectOpenHashMap<>();
        for (String s : graph.keySet()) {
            Object2IntOpenHashMap<String> current = graph.get(s);
            Pair[] p = new Pair[current.size()];
            Object2DoubleOpenHashMap<String> p2 = new Object2DoubleOpenHashMap<>();
            int i = 0;
            int curr_tot = 0;
            int tt = tot.getInt(s);
            for (String ss : current.keySet()) {
                curr_tot += current.getInt(ss);
                p[i++] = new Pair(ss, curr_tot / (tt * 1.));
                p2.put(ss, current.getInt(ss) / (tt * 1.));
            }
            graphComplete.put(s, p);
            graphComplete2.put(s, p2);
        }
        graph = null;
        tot = null;
        System.gc();
    }

    void loadPaths(String file, int k, int h) {
        patterns = new Object2IntOpenHashMap<>();
        patternsIndex = new Object2IntOpenHashMap<>();
        dataset = new ObjectArrayList<>();
        prob = new Object2DoubleOpenHashMap<>();
        try {
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line;
            int index = 0;
            while ((line = br.readLine()) != null) {
                String sp = line.split("-2")[0];
                String[] items = sp.split(" -1 ");
                if (items.length > h) {
                    dataset.add(sp);
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
                            int first = pattern.indexOf(" ");
                            pattern = new StringBuilder(pattern.substring(first + 1));
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

    static void loadStart(int k, int h) {
        starts = new Object2IntOpenHashMap<>();
        for (String line : dataset) {
            String[] items = line.split(" -1 ");
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
        dataset = null;
        System.gc();
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

    static double[] compute(String dataset, int[] seed, int T, int k, int h) throws IOException{
        double[] res = new double[4];
        HashMap<String,double[]> pMCOver = new HashMap<>();
        HashMap<String,double[]> pMCUnder = new HashMap<>();
        HashMap<String,Double> pBinOver = new HashMap<>();
        HashMap<String,Double> pBinUnder = new HashMap<>();
        for(int i = 0; i < 5; i++) {
            String fileInOver = "res/PV/BinVsMC_" + dataset + "_" + seed[i] + "_" + T + "_" + k + "_" + h + "_OVER.txt";
            String fileInUnder = "res/PV/BinVsMC_" + dataset + "_" + seed[i] + "_" + T + "_" + k + "_" + h + "_UNDER.txt";
            FileReader fr = new FileReader(fileInOver);
            BufferedReader br = new BufferedReader(fr);
            String line;
            while ((line = br.readLine()) != null) {
                String p = line.split(" Freq:")[0];
                double pBin = Double.parseDouble(line.split("P-value_Bin: ")[1].split(" P-value_MC: ")[0]);
                if (!pBinOver.containsKey(p)) pBinOver.put(p, pBin);
                double pMC = Double.parseDouble(line.split("P-value_MC: ")[1]);
                if (!pMCOver.containsKey(p)) {
                    double[] arr = new double[5];
                    arr[i] = pMC;
                    pMCOver.put(p, arr);
                } else pMCOver.get(p)[i] = pMC;

            }
            br.close();
            fr.close();
            fr = new FileReader(fileInUnder);
            br = new BufferedReader(fr);
            while ((line = br.readLine()) != null) {
                String p = line.split(" Freq:")[0];
                double pBin = Double.parseDouble(line.split("P-value_Bin: ")[1].split(" P-value_MC: ")[0]);
                if (!pBinUnder.containsKey(p)) pBinUnder.put(p, pBin);
                double pMC = Double.parseDouble(line.split("P-value_MC: ")[1]);
                if (!pMCUnder.containsKey(p)) {
                    double[] arr = new double[5];
                    arr[i] = pMC;
                    pMCUnder.put(p, arr);
                } else pMCUnder.get(p)[i] = pMC;
            }
            br.close();
            fr.close();
        }
        double maxBinO = 0;
        double minBinO = Double.MAX_VALUE;
        double maxBinU = 0;
        double minBinU = Double.MAX_VALUE;
        double maxMCO = 0;
        double minMCO = Double.MAX_VALUE;
        double maxMCU = 0;
        double minMCU = Double.MAX_VALUE;

        for(String p : pBinOver.keySet()){
            double pBin = pBinOver.get(p);
            double[] pMC = pMCOver.get(p);
            Arrays.sort(pMC);
            double c1;
            double c2;
            if(Math.abs(pBin-pMC[0]) > Math.abs(pBin-pMC[4])){
                c1 = pBin/pMC[0];
            }
            else c1 = pBin/pMC[4];
            if(c1<1) c1 = 1/c1;
            double minDiff = Math.abs(pBin-pMC[0]);
            int index = 0;
            for(int j = 1; j < 5; j++){
                if(minDiff>Math.abs(pBin-pMC[j])){
                    minDiff = Math.abs(pBin-pMC[j]);
                    index = j;
                }
            }
            c2 = pBin/pMC[index];
            if(c2<1) c2 = 1/c2;
            double cMaxB = c1;
            double cMinB = c2;
            c1 = pMC[4] / pMC[0];
            c2 = pMC[1] / pMC[0];
            for(int j = 2; j < 5; j++){
                if(pMC[j]/pMC[j-1]<c2) c2 = pMC[j]/pMC[j-1];
            }
            double cMaxM = c1;
            double cMinM = c2;

            if(pBin>=1./(T+1)) {
                if (cMaxB > maxBinO) maxBinO = cMaxB;
                if (cMinB < minBinO) minBinO = cMinB;
                if (cMaxM > maxMCO) maxMCO = cMaxM;
                if (cMinM < minMCO) minMCO = cMinM;
            }
        }

        for(String p : pBinOver.keySet()){
            double pBin = pBinUnder.get(p);
            double[] pMC = pMCUnder.get(p);
            Arrays.sort(pMC);
            double c1;
            double c2;
            if(Math.abs(pBin-pMC[0]) > Math.abs(pBin-pMC[4])){
                c1 = pBin/pMC[0];
            }
            else c1 = pBin/pMC[4];
            if(c1<1) c1 = 1/c1;
            double minDiff = Math.abs(pBin-pMC[0]);
            int index = 0;
            for(int j = 1; j < 5; j++){
                if(minDiff>Math.abs(pBin-pMC[j])){
                    minDiff = Math.abs(pBin-pMC[j]);
                    index = j;
                }
            }
            c2 = pBin/pMC[index];
            if(c2<1) c2 = 1/c2;
            double cMaxB = c1;
            double cMinB = c2;
            c1 = pMC[4] / pMC[0];
            c2 = pMC[1] / pMC[0];
            for(int j = 2; j < 5; j++){
                if(pMC[j]/pMC[j-1]<c2) c2 = pMC[j]/pMC[j-1];
            }
            double cMaxM = c1;
            double cMinM = c2;
            if(pBin>=1./(T+1)) {
                if (cMaxB > maxBinU) maxBinU = cMaxB;
                if (cMinB < minBinU) minBinU = cMinB;
                if (cMaxM > maxMCU) maxMCU = cMaxM;
                if (cMinM < minMCU) minMCU = cMinM;
            }
        }
        res[0] = Math.max(maxBinO,maxBinU);
        res[1] = Math.min(minBinO,minBinU);
        res[2] = Math.max(maxMCO,maxMCU);
        res[3] = Math.min(minMCO,minMCU);
        return res;
    }

    public static void main(String[] args) throws IOException {
        String dataset = args[0];
        int M = Integer.parseInt(args[1]);
        int parallelization = Integer.parseInt(args[2]);
        int[] seed = {0, 100, 200, 300, 400};
        String fileIn = "data/" + dataset + ".csv";
        System.out.println("Dataset: " + dataset);
        System.out.println("M: " + M);
        System.out.println("Parallelization: " + parallelization);
        Path folder = Paths.get("res/PV/");
        try {
            Files.createDirectories(folder);
        }
        catch (IOException e){
            System.out.println("Error Creating Folders \"res/PV/\"!");
            System.exit(1);
        }
        for (int i = 0; i < seed.length; i++) {
            for (int k = 2; k < 6; k++) {
                for (int h = 1; h < k; h++) {
                    System.out.println("Seed: " + seed[i] + " k: " + k + " h: " + h);
                    String fileOutOver = "res/PV/BinVsMC_" + dataset + "_" + seed[i] + "_" + M + "_" + k + "_" + h + "_OVER.txt";
                    System.out.println("FileOutOVER: " + fileOutOver);
                    String fileOutUnder = "res/PV/BinVsMC_" + dataset + "_" + seed[i] + "_" + M + "_" + k + "_" + h + "_UNDER.txt";
                    System.out.println("FileOutUNDER: " + fileOutUnder);
                    long start = System.currentTimeMillis();
                    SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("Algo").set("spark.executor.memory", "5g")
                            .set("spark.driver.memory", "5g").set("spark.executor.heartbeatInterval", "10000000")
                            .set("spark.network.timeout", "10000000").set("spark.storage.blockManagerSlaveTimeoutMs", "10000000").set("spark.driver.bindAddress", "127.0.0.1");
                    JavaSparkContext scc = new JavaSparkContext(sparkConf);
                    scc.setLogLevel("ERROR");
                    TestPValues pvTest = new TestPValues(fileIn, fileOutOver, fileOutUnder, scc, seed[i]);
                    pvTest.execute(M, parallelization, k, h);
                    scc.stop();
                    System.out.println("Total Execution Time: " + (System.currentTimeMillis() - start) + " ms");
                    System.gc();
                }
            }
        }
        double maxBin = 0;
        double minBin = Double.MAX_VALUE;
        double maxMC = 0;
        double minMC = Double.MAX_VALUE;
        for (int k = 2; k < 6; k++) {
            for (int h = 1; h < k; h++) {
                double[] res = compute(dataset,seed,M,k,h);
                if(res[0]>maxBin) maxBin = res[0];
                if(res[1]<minBin) minBin = res[1];
                if(res[2]>maxMC) maxMC = res[2];
                if(res[3]<minMC) minMC = res[3];

            }
        }
        System.out.println("MAX BIN: " + maxBin);
        System.out.println("MIN BIN: " + minBin);
        System.out.println("MAX MC: " + maxMC);
        System.out.println("MIN MC: " + minMC);
    }
}

