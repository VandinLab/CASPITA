import it.unimi.dsi.fastutil.objects.*;
import org.apache.commons.math3.distribution.BinomialDistribution;
import java.io.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

public class Caspita_POG_Bin {

    Object2IntOpenHashMap<String> patterns;
    Object2IntOpenHashMap<String> patternsOriginal;
    Object2IntOpenHashMap<String> starts;
    Object2DoubleOpenHashMap<String> prob;
    Object2ObjectOpenHashMap<String, Pair[]> graphComplete;
    Object2ObjectOpenHashMap<String, Object2DoubleOpenHashMap<String>> graphComplete2;
    Object2ObjectOpenHashMap<String, Object2ObjectOpenHashMap<String, Object2DoubleOpenHashMap<String>>> modEdges;
    ObjectArrayList<String> dataset;
    ObjectOpenHashSet<String> vertices;
    String fileIn;
    String fileOut;
    String fileOutOver;
    String fileOutUnder;

    PrintStream originalStream;

    PrintStream silentStream = new PrintStream(new OutputStream(){
        public void write(int b) {
        }
    });

    Caspita_POG_Bin(String fileIn, String fileOutOver, String fileOutUnder, boolean silent) {
        this.fileIn = fileIn;
        this.fileOutOver = fileOutOver;
        this.fileOutUnder = fileOutUnder;
        originalStream = System.out;
        if(silent) System.setOut(silentStream);
    }

    Caspita_POG_Bin(String fileIn, String fileOut, boolean silent, boolean over) {
        this.fileIn = fileIn;
        this.fileOut = fileOut;
        originalStream = System.out;
        if(silent) System.setOut(silentStream);
    }


    void execute(int P, int k, int h, double FWERTh, boolean over) {

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

        System.out.println("Loading Starting Vertices...");
        start = System.currentTimeMillis();
        loadStart(k, h);
        System.out.println("Done in " + (System.currentTimeMillis() - start) + " ms!");

        System.out.println("Constructing " + h + "-th Order Generative Null Model...");
        start = System.currentTimeMillis();
        createGraph(h,k);
        System.out.println("Done in " + (System.currentTimeMillis() - start) + " ms!");

        analizeDataset(k,h);
        analyzeGraph();

        System.out.println("Computing Starting Probabilities...");
        start = System.currentTimeMillis();
        computeProbability(h);
        System.out.println("Done in " + (System.currentTimeMillis() - start) + " ms!");

        System.out.println("Computation P-values Paths...");
        start = System.currentTimeMillis();
        Object2DoubleOpenHashMap<String> pv;
        if(over) pv = computePValuesBinOver(h);
        else pv = computePValuesBinUnder(h);
        System.out.println("Done in " + (System.currentTimeMillis() - start) + " ms!");
        patternsOriginal = patterns;

        double[] minPValues = new double[P];
        for (int i = 0; i < P; i++) {
            start = System.currentTimeMillis();
            System.out.println("Computation WY " + (i + 1) + "...");
            Random r = new Random(i);
            patterns = generatePathsRandom(r, k, h);
            if (patterns.size() == 0) minPValues[i] = FWERTh;
            else {
                computeProbability(h);
                double min_pv;
                if(over) min_pv = computePValuesBinMinOver(h);
                else min_pv = computePValuesBinMinUnder(h);
                minPValues[i] = Math.min(min_pv, FWERTh);
            }
            System.out.println("Done in " + (System.currentTimeMillis() - start) + " ms!");
        }

        Arrays.sort(minPValues);

        double correctedThreshold = minPValues[(int) (P * FWERTh) - 1];


        if(correctedThreshold == minPValues[(int) (P * FWERTh)]){
            int j = (int) (P * FWERTh) - 2;
            while(j > 0 && correctedThreshold == minPValues[j]) j--;
            correctedThreshold = minPValues[j];
        }

        if (correctedThreshold > FWERTh) correctedThreshold = FWERTh;

        if(over) System.out.println("Corrected Treshold Over Represented Paths: " + correctedThreshold);
        else System.out.println("Corrected Treshold Under Represented Paths: " + correctedThreshold);
        write(correctedThreshold, pv, over);
    }


    void execute_both(int P, int k, int h, double FWERTh) {

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

        System.out.println("Loading Starting Vertices...");
        start = System.currentTimeMillis();
        loadStart(k, h);
        System.out.println("Done in " + (System.currentTimeMillis() - start) + " ms!");

        System.out.println("Constructing " + h + "-th Order Generative Null Model...");
        start = System.currentTimeMillis();
        createGraph(h,k);
        System.out.println("Done in " + (System.currentTimeMillis() - start) + " ms!");

        analizeDataset(k,h);
        analyzeGraph();

        System.out.println("Computing Starting Probabilities...");
        start = System.currentTimeMillis();
        computeProbability(h);
        System.out.println("Done in " + (System.currentTimeMillis() - start) + " ms!");

        System.out.println("Computation P-values Paths...");
        start = System.currentTimeMillis();
        Object2DoubleOpenHashMap<String>[] pv = computePValuesBin(h);
        System.out.println("Done in " + (System.currentTimeMillis() - start) + " ms!");
        patternsOriginal = patterns;

        double[] minPValues_over = new double[P];
        double[] minPValues_under = new double[P];
        for (int i = 0; i < P; i++) {
            start = System.currentTimeMillis();
            System.out.println("Computation WY " + (i + 1) + "...");
            Random r = new Random(i);
            patterns = generatePathsRandom(r, k, h);
            if (patterns.size() == 0) {
                minPValues_over[i] = FWERTh;
                minPValues_under[i] = FWERTh;
            } else {
                computeProbability(h);
                double[] min_pv = computePValuesBinMin(h);
                minPValues_over[i] = Math.min(min_pv[0], FWERTh);
                minPValues_under[i] = Math.min(min_pv[1], FWERTh);
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
        write(correctedThreshold_over, correctedThreshold_under, pv);
    }

    void analizeDataset(int k, int h) {
        double avg = 0;
        int tot = 0;
        int totH = 0;
        int n = 0;
        int l = 0;
        for (String s : dataset) {
            String[] split = s.split(" ");
            n++;
            avg += (split.length - 1);
            if (split.length > h) totH += (split.length - h);
            if (split.length > k) tot += (split.length - k);
            if (split.length > l) l = split.length - 1;
        }
        System.out.println("# of Total Paths of Length k = " + k + " in the Real Dataset: " + tot);
        System.out.println("# of Total Patterns of Length h = " + h + " in the Real Dataset: " + totH);
        System.out.println("# of Transactions of Length > " + h + ": " + n);
        System.out.println("AVG Transactions Length: " + (avg / n));
        System.out.println("Maximum Transactions Length: " + l);
        dataset = null;
        System.gc();
    }

    void write(double correctedTh, Object2DoubleOpenHashMap<String> pv, boolean over) {
        try {
            ObjectArrayList<Triple> finalPatterns = new ObjectArrayList<>();
            for (String p : pv.keySet()) {
                double pval = pv.getDouble(p);
                if (pval < correctedTh) finalPatterns.add(new Triple(p, patternsOriginal.getInt(p), pval));
            }
            if(over) System.out.println("# of Significant Over Represented Paths: " + finalPatterns.size());
            else System.out.println("# of Significant Under Represented Paths: " + finalPatterns.size());

            if(finalPatterns.size()>0) {
                Collections.sort(finalPatterns);
                FileWriter fw = new FileWriter(fileOut);
                BufferedWriter bw = new BufferedWriter(fw);
                for (Triple t : finalPatterns) {
                    bw.write(t.patt + " #Freq: " + t.freq + " #P-value: " + ((t.pValue == 0) ? "~" : "") + t.pValue + "\n");
                }
                bw.close();
                fw.close();
            }
        } catch (IOException e) {
            System.err.println("Output File Error!");
            System.exit(1);
        }
    }

    void write(double correctedTh_over, double correctedTh_under, Object2DoubleOpenHashMap<String>[] pv) {
        try {
            ObjectArrayList<Triple> finalPatterns_over = new ObjectArrayList<>();
            ObjectArrayList<Triple> finalPatterns_under = new ObjectArrayList<>();
            for (String p : pv[0].keySet()) {
                double pv_over = pv[0].getDouble(p);
                double pv_under = pv[1].getDouble(p);
                if (pv_over < correctedTh_over) finalPatterns_over.add(new Triple(p, patternsOriginal.getInt(p), pv_over));
                if (pv_under < correctedTh_under) finalPatterns_under.add(new Triple(p, patternsOriginal.getInt(p), pv_under));
            }
            System.out.println("# of Significant Over Represented Paths: " + finalPatterns_over.size());
            System.out.println("# of Significant Under Represented Paths: " + finalPatterns_under.size());

            if(finalPatterns_over.size()>0) {
                Collections.sort(finalPatterns_over);
                FileWriter fw = new FileWriter(fileOutOver);
                BufferedWriter bw = new BufferedWriter(fw);
                for (Triple t : finalPatterns_over) {
                    bw.write(t.patt + " #Freq: " + t.freq + " #P-value: " + ((t.pValue == 0) ? "~" : "") + t.pValue + "\n");
                }
                bw.close();
                fw.close();
            }

            if(finalPatterns_under.size()>0) {
                Collections.sort(finalPatterns_under);
                FileWriter fw = new FileWriter(fileOutUnder);
                BufferedWriter bw = new BufferedWriter(fw);
                for (Triple t : finalPatterns_under) {
                    bw.write(t.patt + " #Freq: " + t.freq + " #P-value: " + ((t.pValue == 0) ? "~" : "") + t.pValue + "\n");
                }
                bw.close();
                fw.close();
            }

        } catch (IOException e) {
            System.err.println("Output File Error!");
            System.exit(1);
        }
    }

    private Object2IntOpenHashMap<String> generatePathsRandom(Random r, int k, int h) {
        Object2IntOpenHashMap<String> pattRandom = new Object2IntOpenHashMap<>();
        int tot = 0;
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
                    tot++;
                    i++;
                }
            }
        }
        System.out.println("# of Total Paths of Length k = " + k + " in This Random Dataset: " + tot);
        System.out.println("# of Distinct Paths of Length k = " + k + " in This Random Dataset: " + pattRandom.size());
        return pattRandom;
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
                prob.put(s, Math.min(p,1.));
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

    Object2DoubleOpenHashMap<String> computePValuesBinOver(int h) {
        Object2DoubleOpenHashMap<String> pValues = new Object2DoubleOpenHashMap();
        for (String s : patterns.keySet()) {
            String[] ss = s.split(" ");
            StringBuilder prev = new StringBuilder(ss[0]);
            for (int i = 1; i < h; i++) prev.append(" ").append(ss[i]);
            double p = prob.getDouble(s);
            int n = starts.getInt(prev.toString());
            int f = patterns.getInt(s);
            BinomialDistribution b = new BinomialDistribution(n, p);
            pValues.put(s, 1 - b.cumulativeProbability(f - 1));
        }
        return pValues;
    }

    Object2DoubleOpenHashMap<String> computePValuesBinUnder(int h) {
        Object2DoubleOpenHashMap<String> pValues = new Object2DoubleOpenHashMap();
        for (String s : patterns.keySet()) {
            String[] ss = s.split(" ");
            StringBuilder prev = new StringBuilder(ss[0]);
            for (int i = 1; i < h; i++) prev.append(" ").append(ss[i]);
            double p = prob.getDouble(s);
            int n = starts.getInt(prev.toString());
            int f = patterns.getInt(s);
            BinomialDistribution b = new BinomialDistribution(n, p);
            pValues.put(s, b.cumulativeProbability(f));
        }
        return pValues;
    }

    double[] computePValuesBinMin(int h) {
        double[] min = new double[2];
        min[0] = 1.;
        min[1] = 1.;
        for (String s : patterns.keySet()) {
            String[] ss = s.split(" ");
            StringBuilder prev = new StringBuilder(ss[0]);
            for (int i = 1; i < h; i++) prev.append(" ").append(ss[i]);
            double p = prob.getDouble(s);
            int n = starts.getInt(prev.toString());
            int f = patterns.getInt(s);
            BinomialDistribution b = new BinomialDistribution(n, p);
            double pvalue = 1 - b.cumulativeProbability(f - 1);
            if (pvalue < min[0]) min[0] = pvalue;
            pvalue = b.cumulativeProbability(f);
            if (pvalue < min[1]) min[1] = pvalue;
        }
        return min;
    }

    double computePValuesBinMinOver(int h) {
        double min = 1.;
        for (String s : patterns.keySet()) {
            String[] ss = s.split(" ");
            StringBuilder prev = new StringBuilder(ss[0]);
            for (int i = 1; i < h; i++) prev.append(" ").append(ss[i]);
            double p = prob.getDouble(s);
            int n = starts.getInt(prev.toString());
            int f = patterns.getInt(s);
            BinomialDistribution b = new BinomialDistribution(n, p);
            double pvalue = 1 - b.cumulativeProbability(f - 1);
            if (pvalue < min) min = pvalue;
        }
        return min;
    }

    double computePValuesBinMinUnder(int h) {
        double min = 1.;
        for (String s : patterns.keySet()) {
            String[] ss = s.split(" ");
            StringBuilder prev = new StringBuilder(ss[0]);
            for (int i = 1; i < h; i++) prev.append(" ").append(ss[i]);
            double p = prob.getDouble(s);
            int n = starts.getInt(prev.toString());
            int f = patterns.getInt(s);
            BinomialDistribution b = new BinomialDistribution(n, p);
            double pvalue = b.cumulativeProbability(f);
            if (pvalue < min) min = pvalue;
        }
        return min;
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
        System.out.println("# of Vertices: " + vertices.size());
        System.out.println("# of Edges: " + (int)sum);
    }

    void createGraph(int h, int k) {
        Object2IntOpenHashMap<String> tot = new Object2IntOpenHashMap<>();
        Object2ObjectOpenHashMap<String, Object2IntOpenHashMap<String>> graph = new Object2ObjectOpenHashMap<>();
        vertices = new ObjectOpenHashSet<>();
        for (String line : dataset) {
            String[] items = line.split(" ");
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

        if (vertices.size() != graph.size() && k!=h) {
            System.out.println("Computing Modified Edges...");
            long start = System.currentTimeMillis();
            computeModEdges(k, h,tot,graph);
            System.out.println("Done in " + (System.currentTimeMillis() - start) + " ms!");
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
    }

    void computeModEdges(int k, int h, Object2IntOpenHashMap<String> tot, Object2ObjectOpenHashMap<String, Object2IntOpenHashMap<String>> graph) {
        modEdges = new Object2ObjectOpenHashMap<>();
        for (String start : starts.keySet()) {
            Object2ObjectOpenHashMap<String, Object2DoubleOpenHashMap<String>> currMap = new Object2ObjectOpenHashMap();
            ObjectArrayList<String>[] stack = new ObjectArrayList[k - h + 1];
            ObjectOpenHashSet<String>[] remove = new ObjectOpenHashSet[k - h];
            int[] sum = new int[k - h];
            String[] actual = new String[k - h + 1];
            stack[0] = new ObjectArrayList<>();
            for (int i = 0; i < k - h; i++) {
                stack[i + 1] = new ObjectArrayList<>();
                remove[i] = new ObjectOpenHashSet<>();
            }
            actual[0] = start;
            Object2IntOpenHashMap<String> current = graph.get(start);
            int level = 1;
            for (String s : current.keySet()) stack[1].add(s);
            while (level > 0) {
                if (stack[level].isEmpty()) {
                    level--;
                    if (remove[level].size() > 0) {
                        String prev = actual[level];
                        if (remove[level].size() == graph.get(prev).size()) {
                            remove[level - 1].add(prev);
                            sum[level - 1] += graph.get(actual[level - 1]).getInt(prev);
                        } else {
                            Object2IntOpenHashMap<String> c = graph.get(prev);
                            int cTot = tot.getInt(prev);
                            Object2DoubleOpenHashMap<String> curr;
                            if (currMap.containsKey(prev)) curr = currMap.get(prev);
                            else curr = new Object2DoubleOpenHashMap<>();
                            for (String s : c.keySet()) {
                                if (!remove[level].contains(s)) curr.put(s + ":" + level, 1.*c.getInt(s) / (cTot-sum[level]));
                            }
                            currMap.put(prev, curr);
                        }
                    }
                    remove[level] = new ObjectOpenHashSet<>();
                    sum[level] = 0;
                } else {
                    String curVert = stack[level].get(stack[level].size() - 1);
                    if (!graph.containsKey(curVert)) {
                        if (level < k - h + 1) {
                            remove[level - 1].add(curVert);
                            sum[level - 1] += graph.get(actual[level - 1]).getInt(curVert);
                        }
                        stack[level].remove(stack[level].size() - 1);
                    }
                    else {
                        if (level < k - h) {
                            current = graph.get(curVert);
                            actual[level] = curVert;
                            stack[level].remove(stack[level].size() - 1);
                            level++;
                            for (String s : current.keySet()) stack[level].add(s);
                        }
                        else stack[level].remove(stack[level].size() - 1);
                    }

                }
            }
            if (currMap.size() > 0) modEdges.put(start, currMap);
        }
    }

    void loadPaths(String file, int k, int h) {
        patterns = new Object2IntOpenHashMap<>();
        dataset = new ObjectArrayList<>();
        prob = new Object2DoubleOpenHashMap<>();
        try {
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line;
            int tot = 0;
            while ((line = br.readLine()) != null) {
                String[] items = line.split(" ");
                if (items.length > h) {
                    dataset.add(line);
                    if (items.length > k) {
                        StringBuilder pattern = new StringBuilder(items[0]);
                        for (int i = 1; i < k; i++) pattern.append(" ").append(items[i]);
                        for (int j = k; j < items.length; j++) {
                            pattern.append(" ").append(items[j]);
                            int cont = 1;
                            String p = pattern.toString();
                            if (patterns.containsKey(p)) cont = patterns.removeInt(p) + 1;
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
            System.out.println("# of Total Paths of Length k = " + k + " in the Real Dataset: " + tot);
            System.out.println("# of Distinct Paths of Length k = " + k + " in the Real Dataset: " + patterns.size());
        } catch (IOException e) {
            System.err.println("Input File Error!");
            System.exit(1);
        }
    }

    void loadStart(int k, int h) {
        starts = new Object2IntOpenHashMap<>();
        for (String line : dataset) {
            String[] items = line.split(" ");
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

    private class Pair {
        String s;
        double p;

        Pair(String s, double p) {
            this.s = s;
            this.p = p;
        }
    }

    private class Triple implements Comparable<Triple> {
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
        try {
            int P = Integer.parseInt(args[1]);
            int k = Integer.parseInt(args[2]);
            int h = Integer.parseInt(args[3]);
            double FWERTh = Double.parseDouble(args[4]);
            String type = args[5];
            if(h>=k) throw new Exception();
            if(!type.equalsIgnoreCase("both") && !type.equalsIgnoreCase("over") && !type.equalsIgnoreCase("under"))
                throw new Exception();
            System.out.println("Input File: " + dataset);
            System.out.println("P: " + P);
            System.out.println("FWER Threshold: " + FWERTh);
            System.out.println("k: " + k + " h: " + h);
            System.out.println("Type: " + type);
            if(type.equalsIgnoreCase("both")){
                String fileOutOver = args[6];
                String fileOutUnder = args[7];
                System.out.println("Output File Over Represented Paths: " + fileOutOver);
                System.out.println("Output File Under Represented Paths: " + fileOutUnder);
                long start = System.currentTimeMillis();
                Caspita_POG_Bin caspita = new Caspita_POG_Bin(dataset, fileOutOver, fileOutUnder, false);
                caspita.execute_both(P, k, h, FWERTh);
                System.out.println("Total Execution Time: " + (System.currentTimeMillis() - start) + " ms");

            }
            else{
                boolean t = type.equalsIgnoreCase("over");
                String fileOut = args[6];
                if(t) System.out.println("Output File Over Represented Paths: " + fileOut);
                else System.out.println("Output File Under Represented Paths: " + fileOut);
                long start = System.currentTimeMillis();
                Caspita_POG_Bin caspita = new Caspita_POG_Bin(dataset, fileOut, false, t);
                caspita.execute(P, k, h, FWERTh, t);
                System.out.println("Total Execution Time: " + (System.currentTimeMillis() - start) + " ms");
            }
        }
        catch(Exception e){
            System.out.println("Input Format Error!");
            System.exit(1);
        }
    }
}