![LaganLighter](docs/images/lagan.jpg)

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

### Requirements
1. The `libnuma`, `openmp`, and `papi` are required. 
2. A `gcc` with a version greater than 9 are required.
3. For using ParaGrapher, `JDK` with a version greater than 15 is required.
4. `unzip`, `bc`,  and `wget`.

### Compiling and Executing Code
 - Make sure the requried libraries are accessible through `$LD_LIBRARY_PATH`.
 - Run `make alg...` (e.g. `make alg1_sapco_sort`). This builds the executible file and runs it for the test graph. 
 - For identifying input graph and other options, please refer to [LaganLighter Documents, Loading Graphs](docs/0.2-loading.md).
 
### How Does LaganLighter Work?

Please refer to [LaganLighter Documentation](docs/readme.md)

### Supported Graph Types & Loading Graphs
LaganLighter supports reading graphs in *text* format and in compressed *[WebGraph](https://webgraph.di.unimi.it/)* format, using
[ParaGrapher](https://github.com/MohsenKoohi/ParaGrapher) library, 
particularly:
  - *`PARAGRAPHER_CSX_WG_400_AP`*, 
  - *`PARAGRAPHER_CSX_WG_404_AP`*, and
  - *`PARAGRAPHER_CSX_WG_800_AP`* .

Please refer to [Graph Loading Documentation](docs/0.2-loading.md).

### Processing Multiple Graphs 

Please refer to [Launcher Script Documentaion](docs/0.3-launcher.md).
 
### Bugs & Support

If you receive wrong results or you are suspicious about parts of the code, 
please [contact us](https://orcid.org/0000-0002-7465-8003).

### License

Licensed under the GNU v3 General Public License, as published by the Free Software Foundation. 
You must not use this Software except in compliance with the terms of the License. 
Unless required by applicable law or agreed upon in writing, this Software is distributed 
on an "as is" basis, without any warranty; without even the implied warranty of 
merchantability or fitness for a particular purpose, neither express nor implied. 
For details see terms of the License (see attached file: LICENSE). 

#### Copyright 2019-2022, Queen's University of Belfast, Northern Ireland, UK
