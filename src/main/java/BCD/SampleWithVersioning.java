package BCD;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.ArrayList;
import java.util.List;

public class SampleWithVersioning {

    // Each neighboring set is associated with a version number
    class NeighborSetWithVersion {
        int version;
        IntOpenHashSet neighbors;

        public NeighborSetWithVersion(int version, IntOpenHashSet neighbors) {
            this.version   = version;
            this.neighbors = neighbors;
        }
    }

    public Int2ObjectOpenHashMap<List<NeighborSetWithVersion>> nodeToNeighborV;
    IntOpenHashSet verticesToConsolidate = new IntOpenHashSet();


    public SampleWithVersioning() {
        this.nodeToNeighborV  = new Int2ObjectOpenHashMap<>();
    }

    public void addEdgeV(int left, int right, int version) {
        if (!nodeToNeighborV.containsKey(left)) {
            nodeToNeighborV.put(left, new ArrayList<>());
            nodeToNeighborV.get(left).add(new NeighborSetWithVersion(version, new IntOpenHashSet()));
            nodeToNeighborV.get(left).get(0).neighbors.add(right);
        }
        else {
            NeighborSetWithVersion latest = nodeToNeighborV.get(left).get(nodeToNeighborV.get(left).size() - 1);
            IntOpenHashSet newNeighborSet = new IntOpenHashSet(latest.neighbors);
            newNeighborSet.add(right);
            NeighborSetWithVersion newEntry = new NeighborSetWithVersion(version, newNeighborSet);
            nodeToNeighborV.get(left).add(newEntry);
        }

        if (!nodeToNeighborV.containsKey(right)) {
            nodeToNeighborV.put(right, new ArrayList<>());
            nodeToNeighborV.get(right).add(new NeighborSetWithVersion(version, new IntOpenHashSet()));
            nodeToNeighborV.get(right).get(0).neighbors.add(left);
        }
        else {
            NeighborSetWithVersion latest = nodeToNeighborV.get(right).get(nodeToNeighborV.get(right).size() - 1);
            IntOpenHashSet newNeighborSet = new IntOpenHashSet(latest.neighbors);
            newNeighborSet.add(left);
            NeighborSetWithVersion newEntry = new NeighborSetWithVersion(version, newNeighborSet);
            nodeToNeighborV.get(right).add(newEntry);
        }

        // Add vertices to consolidate
        verticesToConsolidate.add(left);
        verticesToConsolidate.add(right);
    }

    public void deleteEdgeV(int left, int right, int version) {
        NeighborSetWithVersion latest = nodeToNeighborV.get(left).get(nodeToNeighborV.get(left).size() - 1);
        IntOpenHashSet newNeighborSet = new IntOpenHashSet(latest.neighbors);
        newNeighborSet.remove(right);
        nodeToNeighborV.get(left).add(new NeighborSetWithVersion(version, newNeighborSet));

        latest = nodeToNeighborV.get(right).get(nodeToNeighborV.get(right).size() - 1);
        newNeighborSet = new IntOpenHashSet(latest.neighbors);
        newNeighborSet.remove(left);
        nodeToNeighborV.get(right).add(new NeighborSetWithVersion(version, newNeighborSet));

        // Add vertices to consolidate
        verticesToConsolidate.add(left);
        verticesToConsolidate.add(right);
    }

    public void exchangeEdgeV(int oldLeft, int oldRight, int left, int right, int version) {
        // delete old
        NeighborSetWithVersion latest;
        IntOpenHashSet newNeighborSet;

        latest = nodeToNeighborV.get(oldLeft).get(nodeToNeighborV.get(oldLeft).size() - 1);
        newNeighborSet = new IntOpenHashSet(latest.neighbors);
        newNeighborSet.remove(oldRight);
        nodeToNeighborV.get(oldLeft).add(new NeighborSetWithVersion(version, newNeighborSet));

        latest = nodeToNeighborV.get(oldRight).get(nodeToNeighborV.get(oldRight).size() - 1);
        newNeighborSet = new IntOpenHashSet(latest.neighbors);
        newNeighborSet.remove(oldLeft);
        nodeToNeighborV.get(oldRight).add(new NeighborSetWithVersion(version, newNeighborSet));

        // add new
        if (!nodeToNeighborV.containsKey(left)) {
            nodeToNeighborV.put(left, new ArrayList<>());
            nodeToNeighborV.get(left).add(new NeighborSetWithVersion(version, new IntOpenHashSet()));
            nodeToNeighborV.get(left).get(0).neighbors.add(right);
        }
        else {
            if (left != oldLeft) {
                NeighborSetWithVersion latest2 = nodeToNeighborV.get(left).get(nodeToNeighborV.get(left).size() - 1);
                IntOpenHashSet newNeighborSet2 = new IntOpenHashSet(latest2.neighbors);
                newNeighborSet2.add(right);
                NeighborSetWithVersion newEntry2 = new NeighborSetWithVersion(version, newNeighborSet2);
                nodeToNeighborV.get(left).add(newEntry2);
            }
            else { // oldLeft == left (entry with the current version has already been created)
                NeighborSetWithVersion latest2 = nodeToNeighborV.get(left).get(nodeToNeighborV.get(left).size() - 1);
                latest2.neighbors.add(right);
            }
        }

        if (!nodeToNeighborV.containsKey(right)) {
            nodeToNeighborV.put(right, new ArrayList<>());
            nodeToNeighborV.get(right).add(new NeighborSetWithVersion(version, new IntOpenHashSet()));
            nodeToNeighborV.get(right).get(0).neighbors.add(left);
        }
        else {
            if (right != oldRight) {
                NeighborSetWithVersion latest2 = nodeToNeighborV.get(right).get(nodeToNeighborV.get(right).size() - 1);
                IntOpenHashSet newNeighborSet2 = new IntOpenHashSet(latest2.neighbors);
                newNeighborSet2.add(left);
                NeighborSetWithVersion newEntry2 = new NeighborSetWithVersion(version, newNeighborSet2);
                nodeToNeighborV.get(right).add(newEntry2);
            }
            else { // oldLeft == left (entry with the current version has already been created)
                NeighborSetWithVersion latest2 = nodeToNeighborV.get(right).get(nodeToNeighborV.get(right).size() - 1);
                latest2.neighbors.add(left);
            }
        }

        // Add vertices that we should consolidate
        verticesToConsolidate.add(oldLeft);
        verticesToConsolidate.add(oldRight);
        verticesToConsolidate.add(left);
        verticesToConsolidate.add(right);
    }

    public void consolidateV() {
        IntSet toBeRemoved = new IntOpenHashSet();

        for (int node : verticesToConsolidate) {
            if (nodeToNeighborV.containsKey(node)) {
                // get the latest entry
                NeighborSetWithVersion latest = nodeToNeighborV.get(node).get(nodeToNeighborV.get(node).size() - 1);
                if (latest.neighbors.isEmpty()) {
                    toBeRemoved.add(node);
                } else {
                    nodeToNeighborV.get(node).clear();
                    nodeToNeighborV.get(node).add(latest);
                    nodeToNeighborV.get(node).get(0).version = 0;
                }
            }
        }

        // Remove all nodes
        for (int node : toBeRemoved) {
            nodeToNeighborV.remove(node);
        }

        // Since we consolidated the nodes, we can now empty the set
        verticesToConsolidate.clear();
    }

}
