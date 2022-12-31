package BCD;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.Callable;

class EdgeTaskVersionedHashMap implements Callable<Double> {
    private final int u;
    private final int v;
    private final boolean add;
    private final long[][] elementsForProbabilityComputation;
    private final int versionNumber;
    private final int k;
    private final SampleWithVersioning hashmapS;

    public EdgeTaskVersionedHashMap(int u, int v, boolean add,
            SampleWithVersioning hashmapS,
            long[][] elementsForProbabilityComputation,
            int versionNumber,
            int memory_budget) {
        this.u = u;
        this.v = v;
        this.add = add;
        this.elementsForProbabilityComputation = elementsForProbabilityComputation;
        this.versionNumber = versionNumber;
        this.k = memory_budget;
        this.hashmapS = hashmapS;
    }

    @Override
    public Double call() throws Exception {
        long ng = elementsForProbabilityComputation[versionNumber][0];
        long nb = elementsForProbabilityComputation[versionNumber][1];
        long s  = elementsForProbabilityComputation[versionNumber][2];
        final double probability = Math.max((s + nb + ng + 0.0) / k * (s + nb + ng - 1.0) / (k - 1.0) * (s + nb + ng - 2.0) / (k - 2.0), 1.0);
        double localInc = add ? probability : -probability;

        double count = countPerEdgeButterfliesSetIntersection(u, v);

        return count * localInc;
    }

    public double countPerEdgeButterfliesSetIntersection(int u, int v) {
        if ((!hashmapS.nodeToNeighborV.containsKey(u) || !hashmapS.nodeToNeighborV.containsKey(v)))
            return 0.0;

        double count = 0;

        IntOpenHashSet uNeighbors = new IntOpenHashSet();
        ListIterator<SampleWithVersioning.NeighborSetWithVersion> iteratorU = hashmapS.nodeToNeighborV.get(u).listIterator(hashmapS.nodeToNeighborV.get(u).size());
        while (iteratorU.hasPrevious()) {
            SampleWithVersioning.NeighborSetWithVersion neighborSetWithVersion = iteratorU.previous();
            if (neighborSetWithVersion.version <= versionNumber) {
                uNeighbors = neighborSetWithVersion.neighbors;
                break;
            }
        }

        IntOpenHashSet vNeighbors = new IntOpenHashSet();
        ListIterator<SampleWithVersioning.NeighborSetWithVersion> iteratorV = hashmapS.nodeToNeighborV.get(v).listIterator(hashmapS.nodeToNeighborV.get(v).size());
        while (iteratorV.hasPrevious()) {
            SampleWithVersioning.NeighborSetWithVersion neighborSetWithVersion = iteratorV.previous();
            if (neighborSetWithVersion.version <= versionNumber) {
                vNeighbors = neighborSetWithVersion.neighbors;
                break;
            }
        }

        uNeighbors.remove(v);
        vNeighbors.remove(u);

        for (int x : uNeighbors) {
            IntOpenHashSet xNeighbors = new IntOpenHashSet();
            ListIterator<SampleWithVersioning.NeighborSetWithVersion> iteratorX = hashmapS.nodeToNeighborV.get(x).listIterator(hashmapS.nodeToNeighborV.get(x).size());
            while (iteratorX.hasPrevious()) {
                SampleWithVersioning.NeighborSetWithVersion neighborSetWithVersion = iteratorX.previous();
                if (neighborSetWithVersion.version <= versionNumber) {
                    xNeighbors = neighborSetWithVersion.neighbors;
                    break;
                }
            }

            IntOpenHashSet common = setIntersection(xNeighbors, vNeighbors);
            count += common.size();
        }

        return count;
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

}

class BatchedEdgesTask implements Callable<Common.Quadruple<Integer, Long, Long, Double>> {
    List<int[]> edges_for_task;
    private final int startVersion;
    private final int endVersion;
    private final long[][] elementsForProbabilityComputation;
    private final int k;
    private final SampleWithVersioning hashmapS;

    private long number_of_set_intersections;
    private long number_of_elements_cheched_in_set_intersections;

    public BatchedEdgesTask(List<int[]> edges_for_task,
                            int startVersion,
                            int endVersion,
                            SampleWithVersioning sampleVersioned,
                            long[][] elementsForProbabilityComputation,
                            int k) {
        this.edges_for_task = edges_for_task;
        this.startVersion   = startVersion;
        this.endVersion     = endVersion;
        this.elementsForProbabilityComputation = elementsForProbabilityComputation;
        this.k = k;
        this.hashmapS = sampleVersioned;
        this.number_of_set_intersections = 0;
        this.number_of_elements_cheched_in_set_intersections = 0;
    }

    @Override
    public Common.Quadruple<Integer, Long, Long, Double> call() throws Exception {
        double finalCount = 0.0;

        int versionNumber = startVersion;
        for (int[] edge : edges_for_task) {
            int u = edge[0];
            int v = edge[1];
            boolean add = edge[2] > 0;

            // incorporate the correct probability
            long ng = elementsForProbabilityComputation[versionNumber][0];
            long nb = elementsForProbabilityComputation[versionNumber][1];
            long s  = elementsForProbabilityComputation[versionNumber][2];
            final double probability = Math.max((s + nb + ng + 0.0) / k * (s + nb + ng - 1.0) / (k - 1.0) * (s + nb + ng - 2.0) / (k - 2.0), 1.0);
            double localInc = add ? probability : -probability;

            double count = countPerEdgeButterfliesSetIntersection(u, v, versionNumber);
            versionNumber++;
            finalCount += count * localInc;
        }

        String[] parts = Thread.currentThread().getName().split("-");
        int thread_number = Integer.parseInt(parts[parts.length - 1]);

        Common.Quadruple<Integer, Long, Long, Double> quadruple_to_return = new Common.Quadruple<Integer, Long, Long, Double>(thread_number, number_of_set_intersections, number_of_elements_cheched_in_set_intersections, finalCount);
        return quadruple_to_return;
    }

    public double countPerEdgeButterfliesSetIntersection(int u, int v, int versionNumber) {
        if ((!hashmapS.nodeToNeighborV.containsKey(u) || !hashmapS.nodeToNeighborV.containsKey(v)))
            return 0.0;

        double count = 0;

        IntOpenHashSet uNeighbors = new IntOpenHashSet();
        ListIterator<SampleWithVersioning.NeighborSetWithVersion> iteratorU = hashmapS.nodeToNeighborV.get(u).listIterator(hashmapS.nodeToNeighborV.get(u).size());
        while (iteratorU.hasPrevious()) {
            SampleWithVersioning.NeighborSetWithVersion neighborSetWithVersion = iteratorU.previous();
            if (neighborSetWithVersion.version <= versionNumber) {
                uNeighbors = neighborSetWithVersion.neighbors;
                break;
            }
        }

        IntOpenHashSet vNeighbors = new IntOpenHashSet();
        ListIterator<SampleWithVersioning.NeighborSetWithVersion> iteratorV = hashmapS.nodeToNeighborV.get(v).listIterator(hashmapS.nodeToNeighborV.get(v).size());
        while (iteratorV.hasPrevious()) {
            SampleWithVersioning.NeighborSetWithVersion neighborSetWithVersion = iteratorV.previous();
            if (neighborSetWithVersion.version <= versionNumber) {
                vNeighbors = neighborSetWithVersion.neighbors;
                break;
            }
        }

        uNeighbors.remove(v);
        vNeighbors.remove(u);

        if (vNeighbors.size() == 0)
            return 0.0;

        for (int x : uNeighbors) {
            // fetch the second neighbors of u
            int xVersion = versionNumber;
            IntOpenHashSet xNeighbors = new IntOpenHashSet();
            if (!hashmapS.nodeToNeighborV.containsKey(x)) continue;
            ListIterator<SampleWithVersioning.NeighborSetWithVersion> iteratorX = hashmapS.nodeToNeighborV.get(x).listIterator(hashmapS.nodeToNeighborV.get(x).size());
            while (iteratorX.hasPrevious()) {
                SampleWithVersioning.NeighborSetWithVersion neighborSetWithVersion = iteratorX.previous();
                if (neighborSetWithVersion.version <= versionNumber) {
                    xNeighbors = neighborSetWithVersion.neighbors;
                    break;
                }
            }

            IntOpenHashSet common = setIntersection(xNeighbors, vNeighbors);
            number_of_set_intersections++;
            number_of_elements_cheched_in_set_intersections += Long.min(xNeighbors.size(), vNeighbors.size());

            count += common.size();
        }

        return count;
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

}