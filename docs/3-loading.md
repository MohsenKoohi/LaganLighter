#  LaganLighter Docs: Graph Types, Loading Graphs & Running Algorithms

## Graph Types

LaganLighter supports the following graph formats:

 - CSR/CSC graph in **textual** format, for testing. This format has 4 lines: 
 	1. The number of vertices (|V|), 
 	2. The number of edges (|E|), 
 	3. |V| space-separated numbers showing offsets of the vertices, and 
 	4. |E| space-separated numbers indicating edges.
 
 - Compressed CSR/CSC graphs in [WebGraph](https://webgraph.di.unimi.it/) format are supported by 
 integrating [ParaGrapher](https://github.com/MohsenKoohi/ParaGrapher) as a submodule.

## Running Algorithms

4 environment variables are passed to `make` to specify the input graph:

  1. `LL_INPUT_GRAPH_PATH`: path to the graph. The default value is `data/test_csr.txt`

  2. `LL_INPUT_GRAPH_TYPE`: type of the graph which can be **`text`**, **`PARAGRAPHER_CSX_WG_400_AP`**, 
  **`PARAGRAPHER_CSX_WG_404_AP`**, or **`PARAGRAPHER_CSX_WG_800_AP`**. The default value is `text`.

  3. `LL_INPUT_GRAPH_IS_SYMMETRIC`: with a value of `0` or `1`, specifies if the 
  input graph is symmetric. The default value is 0.

  4. `LL_STORE_INPUT_GRAPH_IN_SHM`: with a value of `0` or `1`, specifies if it is required 
  to store a copy of the graph as a shared memory object (i.e. in `/dev/shm`). 
  The default value is 0. It is useful when the size of input graph(s) is large and some 
  experiments are repeated multiple times on the graphs. In this case and by storing the graphs as shared memory objects,
  it is not required to load them from the storage.

To run a single algorithm, it is enough to call `make alg...`, e.g., `make alg1_sapco_sort`. 
It runs the algorithm for the default options (stated in the above). To run the algorithm for a particular graph,
you may need to pass the above variables. 
E.g., `LL_INPUT_GRAPH_PATH=data/cnr-2000 LL_INPUT_GRAPH_TYPE=PARAGRAPHER_CSX_WG_400_AP make alg1_sapco_sort` calls the algorithm
for the `data/cnr-2000`. 

Further args such as `no_ht=1` and `debug=1` can be passed to `make` to disable hyperthreading and 
to enable debugging (-g of gcc), respectively.

## How Does LaganLighter Load a Graph?

For *textual* graphs (formatted above), it is required to call `get_ll_400_txt_graph()`. 
To load the graphs in *WebGraph* format (using ParaGrapher), functions `get_ll_400_webgraph()`
and `get_ll_404_webgraph()` should be called. These 3 functions have been defined in `graph.c` and
load the graph in the following steps:

  1. Checking if the graph has been stored as a shared memory object. In that case the graph is returned to the user
  by calling `get_shm_ll_400_graph()` and `get_shm_ll_404_graph()`. This graph should be released by calling 
  `release_shm_ll_400_graph()` or `release_shm_ll_404_graph()`.
	
  2. If the graph is not found in the shared memory, the graph is loaded/decompressed from the secondary storage and
  a NUMA-interleaved memory is allocated for the graph. This graph should be released by calling
  `release_numa_interleaved_ll_400_graph()` or `release_numa_interleaved_ll_404_graph()`.

  3. When the graph is loaded/decompressed from secondary storage, the OS caches some contents of the graph
  in memory. This cached data may impact the performance of memory accesses especially when a large percentage of the
  memory is used. To prevent this effect, the `flushcache.sh` script is called at the end of graph loading.
  If the user has sudo access, a script can be added for this purpose (Please refer to the comments of `flushcache.sh`).

## Where Are The Public Graphs Found?

Please refer to [https://blogs.qub.ac.uk/DIPSA/graphs-list-2024](https://blogs.qub.ac.uk/DIPSA/graphs-list-2024).

You may find WebGraphs in:

  - [https://law.di.unimi.it/datasets.php](https://law.di.unimi.it/datasets.php)
  - [http://madata.bib.uni-mannheim.de/50/](http://madata.bib.uni-mannheim.de/50/) \[DOI: [10.7801/50](https://doi.org/10.7801/50)\]
    - [http://webdatacommons.org/hyperlinkgraph/2012-08/download.html](http://webdatacommons.org/hyperlinkgraph/2012-08/download.html)
    - [http://webdatacommons.org/hyperlinkgraph/2014-04/download.html](http://webdatacommons.org/hyperlinkgraph/2014-04/download.html)
  - [https://docs.softwareheritage.org/devel/swh-dataset/graph/dataset.htm](https://docs.softwareheritage.org/devel/swh-dataset/graph/dataset.html)
  - [https://blogs.qub.ac.uk/DIPSA/MS-BioGraphs](https://blogs.qub.ac.uk/dipsa/ms-biographs/) \[DOI: [10.21227/gmd9-1534](https://doi.org/10.21227/gmd9-1534)\]



--------------------
