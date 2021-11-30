import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;

public class Wiki {

    static void convert(String fileIn, String fileOut){
        try {
            FileReader fr = new FileReader(fileIn);
            BufferedReader br = new BufferedReader(fr);
            FileWriter fw = new FileWriter(fileOut);
            BufferedWriter bw = new BufferedWriter(fw);
            String line;
            Object2IntOpenHashMap<String> vertexes = new Object2IntOpenHashMap<>();
            for (int i = 0; i < 16; i++) line = br.readLine();
            int vertex = 0;
            while ((line = br.readLine()) != null) {
                IntArrayList current = new IntArrayList();
                Scanner sc = new Scanner(line);
                for (int i = 0; i < 3; i++) sc.next();
                String path = sc.next();
                Scanner sc2 = new Scanner(path).useDelimiter(";");
                int pos = 0;
                while (sc2.hasNext()) {
                    String v = sc2.next();
                    if (v.equals("<")) {
                        int index = current.getInt(pos);
                        current.add(index);
                        bw.write(index + " -1 ");
                        pos--;
                    } else {
                        int index = vertex;
                        if (vertexes.containsKey(v)) index = vertexes.getInt(v);
                        else vertexes.put(v, vertex++);
                        current.add(index);
                        bw.write(index + " -1 ");
                        pos = current.size() - 2;
                    }
                }
                bw.write("-2\n");
            }
            bw.close();
            fw.close();
            br.close();
            fr.close();
        }
        catch(IOException e){
            System.out.println("Error Reading and Writing " + fileIn + " and " + fileOut + "!");
            System.exit(1);
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
        convert("original/paths_finished.tsv","data/wiki.csv");
    }
}
