package BCD;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class AbacusExperiments {

    public static Method obtainMethod(String method, Class bcd) throws NoSuchMethodException {
        // Making sure the method exists
        Method countingMethod = null;
        for (Method m : bcd.getDeclaredMethods()) {
            if (m.getName().equals(method)) {
                countingMethod = m;
            }
        }
        if (countingMethod == null) {
            System.out.print("The method " + method + " is not valid");
            System.exit(-1);
        }
        return countingMethod;
    }

    public static void main(String[] args) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException {
        // Input parameters: budget, method and dataset to be used
        int budget     = Integer.parseInt(args[0]);
        String method  = args[1];
        String dataset = args[2];
        String action  = args[3];
        int step       = Integer.parseInt(args[4]);
        int seed       = Integer.parseInt(args[5]);

        final String delim = "[\\s,]+";
        Method countingMethod = obtainMethod(method, Abacus.class);
        // Buffered reader for the input data
        String dataName = dataset.split("_")[0].split("\\.")[0];
        final String dataPath = "../../../datasets/" + dataName + "/" + dataset;
        BufferedReader reader = new BufferedReader(new FileReader(dataPath));

        // Some prints for testing purposes
        System.out.println("budget: "   + budget);
        System.out.println("dataPath: " + dataPath);
        System.out.println("action: "   + action);
        System.out.println("step: "     + step);
        System.out.println("seed: "     + seed);

        final Abacus module = new Abacus(budget, countingMethod, 1, 666, seed);

        /**************************
         * Data Streaming Simulation
         ***************************/

        int  edgeCount = 0; // edge steam count
        long executing = 0; // total time to processing
        while (true)
        {
            final String line = reader.readLine();      // read the edge
            if (line == null)                           // if the line is null the file is done
                break;                                  // end of graph stream

            edgeCount++;                                // increase edge stream count

            int[] edge = Common.parseEdge(line, delim); // Parse the edge

            // Measure total time to process each edge
            long start = System.nanoTime();
            if (edge[2] > 0) {
                // edge insertion
                module.processEdgeAbacus(edge[0], -edge[1], edge[2] > 0);
            }
            else {
                // edge deletion
                module.processEdgeAbacus(edge[0], -edge[1], edge[2] > 0);
            }
            executing += System.nanoTime() - start;

            if(edgeCount % step == 0)
                System.out.println(edgeCount + " " + module.getButterflyCount());
        }

        double totalRunningTimeInSeconds = (double) executing / 1000000000;
        System.out.println("The total time (in secs) is: " + totalRunningTimeInSeconds);

        reader.close();
        module.closeExecutor();
    }
}
