package BCD;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

public class Sample {
    public Int2ObjectOpenHashMap<IntOpenHashSet> nodeToNeighbor; // hashmap[node id] -> {neighbors' ids}

    public Sample() {
        this.nodeToNeighbor = new Int2ObjectOpenHashMap<>();
    }

    // put each node into the other's neighbor set
    public void addEdge(int left, int right) {
        // if the node is not in the sample then create an entry for it
        if (!nodeToNeighbor.containsKey(left))
            nodeToNeighbor.put(left, new IntOpenHashSet());
        nodeToNeighbor.get(left).add(right);

        if (!nodeToNeighbor.containsKey(right))
            nodeToNeighbor.put(right, new IntOpenHashSet());
        nodeToNeighbor.get(right).add(left);
    }

    // delete each node from the other's set of neighbors
    public void deleteEdge(int left, int right) {
        // remove from the hashset of the left
        IntOpenHashSet map = nodeToNeighbor.get(left);
        map.remove(right);
        if (map.isEmpty()) nodeToNeighbor.remove(left);

        // remove from the hashset of the right
        map = nodeToNeighbor.get(right);
        map.remove(left);
        if (map.isEmpty()) nodeToNeighbor.remove(right);
    }

    // checks edge existence in Sample
    public boolean hasEdge(int left, int right) {
        if (nodeToNeighbor.containsKey(left))
            return nodeToNeighbor.get(left).contains(right);
        return false;
    }

    // checks node existence in Sample
    public boolean contains(int node) {
        return nodeToNeighbor.containsKey(node);
    }

    // returns the set of neighbors of a node
    public IntOpenHashSet get(int node) {
        return nodeToNeighbor.get(node);
    }

    // retrieves all nodes' ids
    public IntSet keySet() {
        return nodeToNeighbor.keySet();
    }

    // returns number of edges
    public int getSize() {
        int size = 0;
        for (int node : nodeToNeighbor.keySet())
            // Node ids are greater or equal to 1
            if (node>0)
                size += nodeToNeighbor.get(node).size();
        return size;
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
