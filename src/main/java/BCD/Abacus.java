package BCD;

import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

import java.lang.reflect.Method;
import java.util.Random;
import java.util.concurrent.*;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Abacus {
    public final Sample sample = new Sample();    // graph composed of the sampled edges
    private final SampleWithVersioning sampleVersioned = new SampleWithVersioning(); // graph composed of the sampled edges
    private double butterflyCount = 0; // global butterfly count
    private long s = 0;  // number of current samples
    private int nb = 0;  // number of bad uncompensated deletions
    private int ng = 0;  // number of good uncompensated deletions
    private final int k; // maximum number of samples
    private final int[][] samples; // sampled edges
    private Long2IntOpenHashMap edgeToIndex = new Long2IntOpenHashMap(); // edge to the index of cell that the edge is stored in
    private final Random random;
    private final boolean lowerBound;
    private final int cpus;                   // number of cores in the system
    private final ExecutorService executor;   // used to execute threads
    private final Method countingMethod;
    public final int minibatchSize;
    public final long[][] elementsForProbabilityComputation;
    long[] totalNumberOfSetIntersectionsPerThread;
    long[] totalNumberOfElementsCheckedInSetIntersectionsPerThread;

    public Abacus(final int memoryBudget, final Method method, final int threads, final int minibatchSize, final int seed) {
        this(memoryBudget, method, seed, threads, minibatchSize, true);
    }

    public Abacus(final int memoryBudget, final Method method, final int seed, final int threads, final int minibatchSize, final boolean lowerBound) {
        random = new Random(seed);
        this.k = memoryBudget;
        samples = new int[2][this.k];
        this.lowerBound = lowerBound;
        this.cpus     = threads;
        this.executor = Executors.newFixedThreadPool(cpus);
        this.countingMethod = method;
        this.minibatchSize = minibatchSize;
        this.elementsForProbabilityComputation = new long[this.minibatchSize][3]; // for calculating probability for each sample version { cg | cb | s }
        Arrays.stream(this.elementsForProbabilityComputation).forEach(a -> Arrays.fill(a, 0)); // initialize to 0
        totalNumberOfSetIntersectionsPerThread = new long[this.cpus];
        totalNumberOfElementsCheckedInSetIntersectionsPerThread = new long[this.cpus];
        for (int i = 0; i < this.cpus; i++) {
            totalNumberOfSetIntersectionsPerThread[i] = 0;
            totalNumberOfElementsCheckedInSetIntersectionsPerThread[i] = 0;
        }
     }

    public void processEdgeAbacus(int left, int right, boolean add) throws InvocationTargetException, IllegalAccessException {
        // ignore self loops
        if (left == right) return;
        // assuming undirected graphs, always left < right
        if (left > right) {
            int temp = left;
            left = right;
            right = temp;
        }

        count(left, right, add);

        // edge insertion
        if (add) {
            // increase the number of edges in the stream
            s++;

            // If there is no deletion to compensate
            if (ng + nb == 0) {
                // If it fits, add it
                if (edgeToIndex.size() < k) {
                    addEdge(left, right);
                }
                // If it does not fit, flip a Bernoulli coin (budget/|number of existing edges|)
                else if (random.nextDouble() < (double) k/(s+0.0)) {
                    // keep it by replacing a random edge
                    int index = random.nextInt(edgeToIndex.size());
                    deleteEdge(samples[0][index], samples[1][index]); // remove a random edge from the samples
                    addEdge(left, right); // store the sampled edge
                }
            }
            else if(random.nextDouble() < nb / (nb + ng + 0.0)) {
                addEdge(left, right); // store the sampled edge
                nb--;
            }
            else {
                ng--;
            }
        }
        // edge deletion
        else {
            // decrease the number of edges in the stream
            s--;

            long key = ((long)left * Integer.MAX_VALUE) + right;
            if(edgeToIndex.containsKey(key)) {
                deleteEdge(left, right); // remove the edge from the samples
                nb++;
            }
            else {
                ng++;
            }
        }
    }

    public void processEdgesParabacus(List<int[]> edgesBatch) throws InvocationTargetException, IllegalAccessException {
        List<int[]> processedEdgesMiniBatch = new ArrayList<>();
        // Traverse the edges in the batch from oldest to newest and apply the sampling algorithm
        for (int[] edge : edgesBatch) {
            int left  = edge[0];
            int right = (-1)*edge[1];  // negative to distinguish from the vertices of the left bipartition
            boolean add = edge[2] > 0; // 1 for insertion, 0 for deletion

            if (left == right)
                continue;

            // assuming undirected graphs, always left < right
            if (left > right) {
                int temp = left;
                left = right;
                right = temp;
            }

            processedEdgesMiniBatch.add(new int[]{left, right, edge[2]});
        }
        if (processedEdgesMiniBatch.isEmpty()) return;

        // Initialize the probability information
        Arrays.stream(this.elementsForProbabilityComputation).forEach(a -> Arrays.fill(a, 0));

        // Store the variables for probability calculation for each version
        elementsForProbabilityComputation[0][0] = ng;
        elementsForProbabilityComputation[0][1] = nb;
        elementsForProbabilityComputation[0][2] = s;
        int indexForProbabilityComputation = 1;
        // initialized for the first version that e1 observes (the first edge in the mini-batch)

        // Apply the sampling scheme for each edge in the batch
        int currentVersion = 1;
        for (int[] edge : processedEdgesMiniBatch) {
            int left  = edge[0];
            int right = edge[1];
            boolean add = edge[2] > 0; // 1 for insertion, 0 for deletion

            // edge insertion
            if (add) {
                // increase the number of edges in the stream
                s++;

                // If there is no deletion to compensate
                if (ng + nb == 0) {
                    // If it fits, add it
                    if (edgeToIndex.size() < k)
                        addEdgeVersioned(left, right, currentVersion);
                    // If it does not fit, flip a Bernoulli coin (budget/|number of existing edges|)
                    else if (random.nextDouble() < (double) k / s) {
                        // keep it by replacing a random edge
                        int index = random.nextInt(edgeToIndex.size());
                        exchangeVersioned(samples[0][index], samples[1][index], left, right, currentVersion);
                    }
                } else if (random.nextDouble() < nb / (nb + ng + 0.0)) {
                    addEdgeVersioned(left, right, currentVersion);
                    nb--;
                } else {
                    ng--;
                }
            }
            // edge deletion
            else {
                // decrease the number of edges in the stream
                s--;
                long key = ((long) left * Integer.MAX_VALUE) + right;
                if (edgeToIndex.containsKey(key)) {
                    deleteEdgeVersioned(left, right, currentVersion);
                    nb++;
                } else {
                    ng++;
                }
            }

            // Store the variables for probability calculation for each version
            if (indexForProbabilityComputation < this.minibatchSize) {
                elementsForProbabilityComputation[indexForProbabilityComputation][0] = ng;
                elementsForProbabilityComputation[indexForProbabilityComputation][1] = nb;
                elementsForProbabilityComputation[indexForProbabilityComputation][2] = s;
                indexForProbabilityComputation++;
            }

            // increase current version number
            currentVersion++;
        }

        // Count the butterflies now that we have the versioned sampling
        countMiniBatchVersioned_Batched(processedEdgesMiniBatch);

        // Consolidate the versions
        sampleVersioned.consolidateV();
    }

    private void addEdgeVersioned(int left, int right, int versionNumber) {
        int sampleNum = edgeToIndex.size();
        samples[0][sampleNum] = left;
        samples[1][sampleNum] = right;

        // generate key
        long key = ((long)left * Integer.MAX_VALUE) + right;

        // assign the key to the row at samples
        edgeToIndex.put(key, sampleNum);

        // actually add the edge to the sample with versioning
        sampleVersioned.addEdgeV(left, right, versionNumber);
    }

    private void deleteEdgeVersioned(int left, int right, int versionNumber) {
        // get the current number of edges
        int sampleNum = edgeToIndex.size();

        // get its row index at Samples
        long key = ((long)left * Integer.MAX_VALUE) + right;
        int index = edgeToIndex.remove(key);

        // delete the edge from sample
        sampleVersioned.deleteEdgeV(left, right, versionNumber);

        // if the edge deleted is not the last one
        if (index < sampleNum - 1) {
            // get the last one in samples and put it in the deleted edges place
            int newSrc = samples[0][index] = samples[0][sampleNum - 1];
            int newDst = samples[1][index] = samples[1][sampleNum - 1];

            // update the position
            long newKey = ((long) newSrc * Integer.MAX_VALUE) + newDst;
            edgeToIndex.put(newKey, index);
        }
    }

    private void exchangeVersioned(int old_left, int old_right, int left, int right, int versionNumber) {
        // first delete
        int sampleNum = edgeToIndex.size();

        // get its row index at Samples
        long key = ((long)old_left * Integer.MAX_VALUE) + old_right;
        int index = edgeToIndex.remove(key);

        // if the edge deleted is not the last one
        if (index < sampleNum - 1) {
            // get the last one in samples and put it in the deleted edges place
            int newSrc = samples[0][index] = samples[0][sampleNum - 1];
            int newDst = samples[1][index] = samples[1][sampleNum - 1];

            // update the position
            long newKey = ((long) newSrc * Integer.MAX_VALUE) + newDst;
            edgeToIndex.put(newKey, index);
        }

        // then add
        int sampleNum2 = edgeToIndex.size();
        samples[0][sampleNum2] = left;
        samples[1][sampleNum2] = right;

        // generate key
        long key2 = ((long)left * Integer.MAX_VALUE) + right;

        // assign the key to the row at samples
        edgeToIndex.put(key2, sampleNum2);

        // finally, create the new version and insert it
        sampleVersioned.exchangeEdgeV(old_left, old_right, left, right, versionNumber);
    }

    private void addEdge(int left, int right) {
        // save in samples at the end
        int sampleNum = edgeToIndex.size();
        samples[0][sampleNum] = left;
        samples[1][sampleNum] = right;

        // generate key
        long key = ((long)left * Integer.MAX_VALUE) + right;

        // assign the key to the row at samples
        edgeToIndex.put(key, sampleNum);

        // actually add the edge to the sample
        sample.addEdge(left, right);
    }

    private void deleteEdge(int left, int right) {
        // get the current number of edges
        int sampleNum = edgeToIndex.size();

        // get its row index at Samples
        long key = ((long)left * Integer.MAX_VALUE) + right;
        int index = edgeToIndex.remove(key);

        // delete the edge from sample
        sample.deleteEdge(left,right);

        // if the edge deleted is not the last one
        if (index < sampleNum - 1) {
            // get the last one in samples and put it in the deleted edges place
            int newSrc = samples[0][index] = samples[0][sampleNum - 1];
            int newDst = samples[1][index] = samples[1][sampleNum - 1];

            // update the position
            long newKey = ((long) newSrc * Integer.MAX_VALUE) + newDst;
            edgeToIndex.put(newKey, index);
        }
    }

    public double getButterflyCount() {
        return butterflyCount;
    }

    public void closeExecutor() {
        this.executor.shutdown();
    }

    private int sumDegrees(IntOpenHashSet nodes) {
        int degree = 0;
        for (int node : nodes)
            degree += sample.get(node).size();
        return degree;
    }

    private int complexity(IntOpenHashSet nodes, int bound) {
        // Computes the number of operations
        int operations = 0;
        // Number of operations if we choose the neighbors of left as starting point
        for (int node : nodes)
            operations += Math.min(sample.get(node).size(), bound);
        return operations;
    }

    private void countMiniBatchVersioned_Batched(List<int[]> minibatch) throws InvocationTargetException, IllegalAccessException {
        // Create the tasks out of the edges in the minibatch
        ArrayList<BatchedEdgesTask> taskListBatchedEdges = new ArrayList<>();
        int num_cpus = this.cpus;
        int num_edges_per_task = minibatch.size() / num_cpus;

        int startVersion = 0;
        int endVersion = num_edges_per_task;
        int task_id = 1;
        List<int[]> edges_for_task = new ArrayList<>();
        for (int[] edge: minibatch) {
            edges_for_task.add(edge);
            if (edges_for_task.size() == num_edges_per_task) {
                // create the task
                taskListBatchedEdges.add(new BatchedEdgesTask(edges_for_task,
                                                              startVersion,
                                                              endVersion,
                                                              sampleVersioned,
                                                              elementsForProbabilityComputation,
                                                              this.k));
                edges_for_task = new ArrayList<>();
                startVersion = endVersion;
                endVersion += num_edges_per_task;
                task_id++;
            }
        }

        if (task_id == this.cpus) {
            System.out.println("Creating task "+task_id+" with "+edges_for_task.size()+" edges");
            taskListBatchedEdges.add(new BatchedEdgesTask(edges_for_task,
                    startVersion,
                    endVersion,
                    sampleVersioned,
                    elementsForProbabilityComputation,
                    this.k));
            edges_for_task = new ArrayList<>();
            startVersion = endVersion;
            endVersion += num_edges_per_task;
            task_id++;
        }

        double count = 0;
        try {
            for (Future<Common.Quadruple<Integer, Long, Long, Double>> doubleFuture : executor.invokeAll(taskListBatchedEdges)) {
                Common.Quadruple<Integer, Long, Long, Double> res = doubleFuture.get();
                count += res.getFourth();
                this.totalNumberOfSetIntersectionsPerThread[res.getFirst()-1] += res.getSecond();
                this.totalNumberOfElementsCheckedInSetIntersectionsPerThread[res.getFirst()-1] += res.getThird();
            }
        } catch (InterruptedException | ExecutionException e) {
        }

        butterflyCount += count;
    }

    private void count(final int left, final int right, boolean add) throws InvocationTargetException, IllegalAccessException {
        // if this edge contains a newly observed node, there cannot be any butterflies
        if (!sample.contains(left) || !sample.contains(right))
            return;

        // Establish whether we are adding or deleting
        final double probability = Math.max((s + nb + ng + 0.0) / k * (s + nb + ng - 1.0) / (k - 1.0) * (s + nb + ng - 2.0) / (k - 2.0), 1);
        double localInc = add ? probability : -probability;

        IntOpenHashSet leftNeighbors  = sample.get(left);
        IntOpenHashSet rightNeighbors = sample.get(right);
        leftNeighbors.remove(right);
        rightNeighbors.remove(left);

        // Update the number of total butterflies & compute the local ones
        Object[] params = new Object[]{left, leftNeighbors, right, rightNeighbors, localInc};
        double temp = (double) countingMethod.invoke(this, params);
        butterflyCount += temp;

        if (butterflyCount < 0 && lowerBound) butterflyCount = 0;
    }

    public IntOpenHashSet setIntersection(IntOpenHashSet set1, IntOpenHashSet set2) {
        IntOpenHashSet result;
        if (set2.size() < set1.size()) {
            result = set2.clone();
            result.retainAll(set1);
        } else {
            result = set1.clone();
            result.retainAll(set2);
        }
        return result;
    }

    private double perEdgeButterflyCountingLeft(int node, IntOpenHashSet neighbors, int other, IntOpenHashSet otherNeighbors, double localInc) {
        double count = 0;

        // Check neighbors of the left node
        for (int neighbor : neighbors) {
            IntOpenHashSet common = setIntersection(sample.get(neighbor), otherNeighbors);

            // Each one is part of a different butterfly:
            double n_tmp = localInc * common.size();
            count += n_tmp;
        }

        return count;
    }

    private double perEdgeButterflyCountingSmart(int left, IntOpenHashSet leftNeighbors, int right, IntOpenHashSet rightNeighbors, double localInc) {
        int useLeft  = complexity(leftNeighbors, rightNeighbors.size());
        int useRight = complexity(rightNeighbors, leftNeighbors.size());
        if (useLeft < useRight) return perEdgeButterflyCountingLeft(left, leftNeighbors, right, rightNeighbors, localInc);
        return perEdgeButterflyCountingLeft(right, rightNeighbors, left, leftNeighbors, localInc);
    }

}
