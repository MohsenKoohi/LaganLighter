![LaganLighter](https://blogs.qub.ac.uk/dipsa/wp-content/uploads/sites/319/2023/02/header-18.jpg)

# [LaganLighter:  Structure-Aware High-Performance Graph Algorithms](https://blogs.qub.ac.uk/DIPSA/LaganLighter/)

This repository contains the shared-memory CPU-based soure code of the LaganLighter project: https://blogs.qub.ac.uk/DIPSA/LaganLighter .   

### Algorithms in This Repo
 - [SAPCo Sort](https://blogs.qub.ac.uk/DIPSA/Sapco-Sort-Optimizing-Degree-Ordering-For-Power-Law-Graphs/): `alg1_sapco_sort`
 - [Thrifty Label Propagation Connected Components](https://blogs.qub.ac.uk/DIPSA/Thrifty-Label-Propagation-Fast-Connected-Components-for-Skewed-Degree-Graphs/): `alg2_thrifty`
 - [MASTIFF: Structure-Aware Mimum Spanning Tree/Forest](https://blogs.qub.ac.uk/DIPSA/MASTIFF-Structure-Aware-Minimum-Spanning-Tree-Forest/): `alg3_mastiff`
 - [iHTL: in-Hub Temporal Locality in SpMV (Sparse-Matrix Vector Multiplication) based Graph Processing](https://blogs.qub.ac.uk/DIPSA/Exploiting-in-Hub-Temporal-Locality-in-SpMV-based-Graph-Processing/): to be added
 - [LOTUS: Locality Optimizing Trinagle Counting](https://blogs.qub.ac.uk/DIPSA/LOTUS-Locality-Optimizing-Triangle-Counting/): to be added

### Cloning 
`git clone https://github.com/MohsenKoohi/LaganLighter.git --recursive`

### Updating through `pull`
- `git pull --recurse-submodules` or
- You may set recursive submodule update globally using `git config --global submodule.recurse true` and then `git pull` fetches all updates.

### How Does It Work?
[LaganLighter source code documentation](https://blogs.qub.ac.uk/DIPSA/LaganLighter-Source-Code)

### Requirements
1. The `libnuma`, `openmp`, and `papi` are required. 
2. A `gcc` with a version greater than 9 are required.
3. For using ParaGrapher, `JDK` with a version greater than 15 is required.
4. `unzip`, `bc`,  and `wget`.

### Compiling and Executing Code
 - Set the path to `gcc` compiler folder in Line 5 of the `Makefile`, if you do not have it in `PATH`.
 - Please modify Line 14, if the required libraries (`libnuma` and `libpapi`) are not in the library path,`LD_LIBRARY_PATH`.
 - Run `make alg...` (e.g. `make alg1_sapco_sort`). This builds the executible file and runs it for the test graph. To run it for other graphs, add the path to your graph as the value to variable `graph` to the `make` (e.g. `make alg1_sapco_sort graph="path/to/graph" graph_type=TYPE`), where `TYPE` can be `text`, or one of the types supported by [ParaGrapher](https://blogs.qub.ac.uk/DIPSA/ParaGrapher), e.g.: **`PARAGRAPHER_CSX_WG_400_AP`**, **`PARAGRAPHER_CSX_WG_404_AP`**, or **`PARAGRAPHER_CSX_WG_800_AP`**. 
 - To disable hyper-threading, please use `no_ht=1` with make.
 - To enable debugging (-g of gcc), use `debug=1`.

### Graph Types
LaganLighter supports  the following graph formats:

 - CSR/CSC graph in textual format, for testing. This format has 4 lines: 
 	1. The number of vertices (|V|), 
 	2. The number of edges (|E|), 
 	3. |V| space-separated numbers showing offsets of the vertices, and 
 	4. |E| space-separated numbers indicating edges.
 
 - Compressed CSR/CSC graphs in WebGraph format are now supported by integrating [ParaGrapher](https://github.com/MohsenKoohi/ParaGrapher) as a submodule.
 
### Bugs & Support
If you receive wrong results or you are suspicious about parts of the code, please [contact us](https://orcid.org/0000-0002-7465-8003) . 

### License
Licensed under the GNU v3 General Public License, as published by the Free Software Foundation. You must not use this Software except in compliance with the terms of the License. Unless required by applicable law or agreed upon in writing, this Software is distributed on an "as is" basis, without any warranty; without even the implied warranty of merchantability or fitness for a particular purpose, neither express nor implied. For details see terms of the License (see attached file: LICENSE). 

#### Copyright 2022 The Queen's University of Belfast, Northern Ireland, UK
