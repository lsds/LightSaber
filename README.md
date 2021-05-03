<div align="center">
<img src="https://github.com/lsds/LightSaber/blob/master/docs/images/logo.png" height="70%" width="70%"></img>
</div>

# LightSaber [![License](https://img.shields.io/github/license/lsds/LightSaber.svg?branch=master)](https://github.com/lsds/LightSaber/blob/master/LICENCE.md)

LightSaber is a stream processing engine that balances parallelism and incremental processing when
executing window aggregation queries on multi-core CPUs. LightSaber operates on in-order
streams of data and achieves up to an order of magnitude higher throughput compared to existing systems.

### Getting started

The `prepare-software.sh` script will guide you through the installation and compilation process of our system locally.
The script is tested on **Ubuntu 18.04.5 LTS**.

```
$ git clone https://github.com/lsds/LightSaber.git
$ cd LightSaber
$ ./scripts/prepare-software.sh
$ ./scripts/build.sh
```

Otherwise use the Dockerfile:
```
$ git clone https://github.com/lsds/LightSaber.git
$ cd LightSaber
$ docker build --tag="lightsaber" .
$ docker run -ti lightsaber
```

### Run unit tests
```
$ ./build/test/unit_tests/unit_tests_run
``` 

### Running a microbenchmark (e.g., Projection)
```
$ ./build/test/benchmarks/microbenchmarks/TestProjection
```

### Running an application benchmark with sample data
```
$ ./build/test/benchmarks/applications/cluster_monitoring
```
### How to cite LightSaber
* **[SIGMOD]** Georgios Theodorakis, Alexandros Koliousis, Peter R. Pietzuch, and Holger Pirk. LightSaber: Efficient Window Aggregation on Multi-core Processors, SIGMOD, 2020
```
@inproceedings{Theodorakis2020,
 author = {Georgios Theodorakis and Alexandros Koliousis and Peter R. Pietzuch and Holger Pirk},
 title = {{LightSaber: Efficient Window Aggregation on Multi-core Processors}},
 booktitle = {Proceedings of the 2020 ACM SIGMOD International Conference on Management of Data},
 series = {SIGMOD '20},
 year = {2020}, 
 publisher = {ACM},
 address = {Portland, OR, USA},
}
```

#### Other related publications
* **[EDBT]** Georgios Theodorakis, Peter R. Pietzuch, and Holger Pirk. SlideSide: A fast Incremental Stream Processing Algorithm for Multiple Queries, EDBT, 2020
* **[ADMS]** Georgios Theodorakis, Alexandros Koliousis, Peter R. Pietzuch, and Holger Pirk. Hammer Slide: Work- and CPU-efficient Streaming Window Aggregation, ADMS, 2018 [[code]](https://github.com/grtheod/Hammerslide)
* **[SIGMOD]** Alexandros Koliousis, Matthias Weidlich, Raul Castro Fernandez, Alexander Wolf, Paolo Costa, and Peter Pietzuch. Saber: Window-Based Hybrid Stream Processing for Heterogeneous Architectures, SIGMOD, 2016


### The LightSaber engine
<div align="center">
<img src="https://github.com/lsds/LightSaber/blob/master/docs/images/architecture.png"></img>
</div>

#### LightSaber configuration

Variables in **SystemConf.h** configure the LightSaber runtime. Each of them also corresponds to a command-line argument available to all LightSaber applications:

###### --threads _N_

Sets the number of CPU worker threads (`WORKER_THREADS` variable). The default value is `1`. **CPU worker threads are pinned to physical cores**. The threads are pinned to core ids based on the underlying hardware (e.g., if there are multiple sockets with n cores each, the first n threads are pinned in the first socket and so on).

###### --slots _N_

Sets the number of intermediate query result slots (`SLOTS` variable). The default value is `256`.

###### --partial-windows _N_

Sets the maximum number of window fragments in a query task (`PARTIAL_WINDOWS` variable). The default value is `1024`.

###### --circular-size _N_

Sets the circular buffer size in bytes (`CIRCULAR_BUFFER_SIZE` variable). The default value is `4194304`, i.e. 4 MB.

###### --unbounded-size _N_

Sets the intermediate result buffer size in bytes (`UNBOUNDED_BUFFER_SIZE` variable). The default value is `524288`, i.e. 512 KB.

###### --hashtable-size _N_

Hash table size (in number of buckets): hash tables hold partial window aggregate results (`HASH_TABLE_SIZE` variable with the default value 512).

###### --performance-monitor-interval _N_

Sets the performance monitor interval, in msec (`PERFORMANCE_MONITOR_INTERVAL` variable). The default value is `1000`, i.e. 1 sec. Controls how often LightSaber prints on standard output performance statistics such as throughput and latency.

###### --latency `true`|`false`

Determines whether LightSaber should measure task latency or not (`LATENCY_ON` variable). The default value is `false`.

###### To enable NUMA-aware scheduling

Set the HAVE_NUMA flag in the respective CMakeLists.txt (e.g., in test/benchmarks/applications/CMakeLists.txt) and recompile the code.