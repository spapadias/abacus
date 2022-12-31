# ABACUS: Counting Butterflies in Fully Dynamic Bipartite Graph Streams
**Abacus** is a novel streaming algorithm for butterfly counting in a fully dynamic graph stream, which entails edge insertions and deletions.
**Abacus** estimates the buttefly counts of by making a single pass over the graph stream.

[//]: # (**Abacus** has the following advantages:)

[//]: # (* *Accurate*: **Abacus** is up to 148x more accurate than the baselines.)

[//]: # (* *Fast*: **Abacus** maintains similar throughput to the baselines.  )

[//]: # (* *Theoretically Sound*: **Abacus** always maintains unbiased estimates.)

# Setting up Abacus
Here, we explain how to setup Abacus in order to run from your machine.

## Hardware Dependencies
Any modern x86-based multicore machine can be used to run Abacus. 

## Software Dependencies
Abacus requires `Java 8` or later. Furthermore, abacus utilizes the [fastutil](https://fastutil.di.unimi.it/) java
library (we use `version 7.2.0` and the jar is included in our repo).

## Datasets
The datasets we used can be found [here](http://www.konect.cc/networks/).

If you want to use your own datasets, create a folder at `datasets/` folder
containing your selected dataset.
* Columns must be separated by whitespace ' '.
* Each row must correspond to an edge (u v o).
* o denotes the operation, if `o = 1` then addition, otherwise if `o = -1` then deletion.
* All nodes in the same column must be from the same partition.

### Producing Datasets with Deletions
To produce a dataset that contains edge deletions follow the next steps:
1. Execute the `preprocess.py` script that is located in `src/makis/python/` folder. By doing this the edge insertionos are
   created (each line has `o = 1`) and any potential comments are removed.
2. Execute the `addDeletions.py` script that is located in `src/makis/python/` folder.
   Select a preprocessed dataset from step 1, and insert the deletions ratio(s) (values must be between 0 and 1).
3. You can find each produced dataset in its corresponding folder.

Note that in the `dataset/` folder we have included a reduced version of the *Movielens* dataset with `20K` edges, which we have
preprocessed (all edges have `o = 1`). 

# Running Abacus
For running Abacus execute the `callAbacus.py` script that is located in the `src/main/python/` folder.
For instance, we show the output of Abacus on *Movielens* with `20K` edges and with `α = 20%`
deletions ratio:

```
Write the location of the data folder from the project root:
datasets

Write the indexes of the datasets (separated by commas)
__MOVIELENS-20K__             
1: movielens-20k_alpha=0.2.dat
2: movielens-20k.dat          
If you want to use all of them just press Enter
1
These are the selected datasets:
 'movielens-20k_alpha=0.2.dat' 
 
Write the memory budgets (separated by commas)
2000
These are the selected ratios:
 '2000' 
 
Write the step for writing the butterfly counts: >? 2000
We write butterfly counts every  2000 edges

Which random seed to use?: >? 0
We use  0 as random seed

Compilation finished successfully
budget: 2000
dataPath: ../../../datasets/movielens-20k/movielens-20k_alpha=0.2.dat
action: Accuracy
step: 2000
seed: 0
Memory budget: 2000

2000 0.0
4000 0.0
6000 21.562272777529905
8000 57.7663334895676
10000 224.2870824826828
12000 456.5520715477859
14000 614.0336150072033
16000 1652.6808603115373
18000 1715.4746259721455
20000 2718.6998390141016
22000 3241.4354696702703
24000 1920.631533191521

The total time (in secs) is: 0.093337963
```

Similarly, for running  Parabacus execute  the `callParabacus.py` script located in the `src/main/python/` folder.