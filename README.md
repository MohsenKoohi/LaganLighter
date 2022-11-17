# [LaganLighter:  Structure-Aware High-Performance Graph Algorithms](https://blogs.qub.ac.uk/GraphProcessing/LaganLighter/)

This repository contains the shared-memory CPU-based soure code of the LaganLighter project: https://blogs.qub.ac.uk/GraphProcessing/LaganLighter .   

## Algorithms in This Repo
 - [SAPCo Sort](https://blogs.qub.ac.uk/GraphProcessing/Sapco-Sort-Optimizing-Degree-Ordering-For-Power-Law-Graphs/): `alg1_sapco_sort` 
 - [Thrifty Label Propagation Connected Components](https://blogs.qub.ac.uk/graphprocessing/Thrifty-Label-Propagation-Fast-Connected-Components-for-Skewed-Degree-Graphs/): `alg2_thrifty`

 - [MASTIFF: Structure-Aware Mimum Spanning Tree/Forest](https://blogs.qub.ac.uk/GraphProcessing/MASTIFF-Structure-Aware-Minimum-Spanning-Tree-Forest/): `alg3_mastiff`

 - [iHTL: in-Hub Temporal Locality For Sparse-Matrix Vectore (SpMV) Multiplication](https://blogs.qub.ac.uk/GraphProcessing/Exploiting-in-Hub-Temporal-Locality-in-SpMV-based-Graph-Processing/): to be added

 - [LOTUS: Locality Optimizing Trinagle Counting](https://blogs.qub.ac.uk/GraphProcessing/LOTUS-Locality-Optimizing-Triangle-Counting/): to be added


## How Does It Work?
[LaganLighter source code documentation](https://blogs.qub.ac.uk/GraphProcessing/LaganLighter-Source-Code)

## Requirements
The `libnuma`, `openmp`, and `papi` are required.

## Compiling and Executing Code
 - Set the path to `gcc` compiler folder in Line 1 of the `Makefile`.
 - Please modify Line 6, if the required libraries are not in the path.
 - Run `make alg...` (e.g. `make alg1_sapco_sort`). This builds the executible file and runs it for the test graph. To run it for other graphs, add the path to your graph as the argument to the `.o` executible file (e.g. `obj/alg1_sapco_sort.o path/to/graph`).
 - To disable hyper-threading, please use `no_ht=1` with make.
 - To enable debugging (-g of gcc), use `debug=1`.

## Graph Types
LaganLighter supports  the following graph formats:
 - CSR/CSC graph in text format, for testing, e.g. `data/test_csr.txt`. This format has 4 lines: |V|, |E|, |V| space-separated offsets, and |E| space-separated edges, respectively.  
 - CSR [WebGraph](https://law.di.unimi.it/datasets.php) format
 - CSR/CSC binary format
 - CSR/CSC sbin (separated binary) format 

## Bugs & Support
As "we write bugs that in special cases work correctly", we try to evaluate and validate the algorithms and their implementation. If you have received wrong results or you are suspicious about parts of the code, please [contact us](https://blogs.qub.ac.uk/GraphProcessing/LaganLighter) or [submit an issue](https://github.com/DIPSA-QUB/LaganLighter/issues). 

## Fundings
The project LaganLighter leading to this Software has been supported by:
 - PhD scholarship from The Department for the Economy, Northern Ireland and The Queen’s University Belfast 
 - Kelvin-2 supercomputer (EPSRC grant EP/T022175/1) 
 - DiPET (CHIST-ERA-18-SDCDN-002, EPSRC grant EP/T022345/1) 

## License
Licensed under the GNU v3 General Public License, as published by the Free Software Foundation. You must not use this Software except in compliance with the terms of the License. Unless required by applicable law or agreed upon in writing, this Software is distributed on an "as is" basis, without any warranty; without even the implied warranty of merchantability or fitness for a particular purpose, neither express nor implied. For details see terms of the License (see attached file: LICENSE). 

#### Copyright 2019-2022 The Queen's University of Belfast, Northern Ireland, UK
