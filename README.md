# Carbon- and Precedence-Aware Scheduling for Data Processing

As large-scale data processing workloads continue to grow, their carbon footprint raises concerns. Prior research on carbon-aware schedulers has focused on shifting computation to align with availability of low-carbon energy, but these approaches assume that each task can be executed independently. 
In contrast, data processing jobs have precedence constraints (i.e., outputs of one task are inputs for another) that complicate decisions, since delaying an upstream "bottleneck" task to a low-carbon period will also block downstream tasks, impacting the entire job's completion time. 
In this paper, we show that carbon-aware scheduling for data processing benefits from knowledge of both time-varying carbon and precedence constraints. 
Our main contribution is `PCAPS`, a carbon-aware scheduler that interfaces with modern ML scheduling policies to explicitly consider the precedence-driven importance of  each task in addition to carbon.  To illustrate the gains due to fine-grained task information, we also study `CAP`, a wrapper for any carbon-agnostic scheduler that adapts the key provisioning ideas of `PCAPS`.
Our schedulers enable a configurable priority between carbon reduction and job completion time, and we give analytical results characterizing the trade-off between the two.
Furthermore, our Spark prototype on a 100-node Kubernetes cluster shows that a moderate configuration of `PCAPS` reduces carbon footprint by up to 32.9% without significantly impacting the cluster's total efficiency.

# Experimental Code

The code in this repository is split between our simulator code (in `simulator/`) and our proof-of-concept prototype code (in `prototype/`).  A high-level summary of the former is listed below, with details in each respective directory.

## Reproducing Simulator Results

To evaluate `PCAPS` and `CAP` against all implemented baselines in a simulated cluster with 100 executors and one trial of 50 random TPC-H jobs under the sample carbon trace, run the following command at the root of this repository:

```bash
python3 simulator/test.py --num_exp 1 --exec_cap 50 --exec_cap 100 --num_init_dags 1 --num_stream_dags 50 --canvs_visualization 0 --test_schemes spark_fifo dynamic_partition decima green_hadoop cap_fifo cap_partition cap_decima pcaps
```

Note that this simulator assumes a Python environment with the following packages:
- [NumPy](https://numpy.org)
- [SciPy](https://scipy.org)
- [pandas](https://pandas.pydata.org)
- [networkx](https://networkx.org)
- [TensorFlow](https://www.tensorflow.org) for Decima inference
- [tf-slim](https://github.com/google-research/tf-slim)
- [Matplotlib](https://matplotlib.org) for creating plots 
- [seaborn](https://seaborn.pydata.org) for creating plots 

# References

**Spark standalone simulator code and Decima:**

Our simulator code and Decima baseline comparison algorithm are both adapted from the following paper.  Find their original simulator code at [https://github.com/hongzimao/decima-sim](https://github.com/hongzimao/decima-sim).

> Hongzi Mao, Malte Schwarzkopf, Shaileshh Bojja Venkatakrishnan, Zili Meng, and Mohammad Alizadeh. 2019. Learning scheduling algorithms for data processing clusters. In Proceedings of the ACM Special Interest Group on Data Communication (SIGCOMM '19). Association for Computing Machinery, New York, NY, USA, 270–288. https://doi.org/10.1145/3341302.3342080

**Carbon intensity data:**

This repository includes a sample of historical carbon intensity data from [Electricity Maps](https://www.electricitymaps.com/), for the `DE` grid region.

> Electricity Maps. retrieved 2024. https://www.electricitymaps.com

**Alibaba cluster traces:**

Our prototype experiments leverage cluster traces released by Alibaba to generate realistic DAG Spark jobs.  These can be found at the following GitHub repository:
> Alibaba. 2018. Cluster data collected from production clusters in Alibaba for cluster management research. [`alibaba/clusterdata/tree/master/cluster-trace-v2018`](https://github.com/alibaba/clusterdata/tree/master/cluster-trace-v2018)

**TPC-H benchmark Spark jobs:**

We use TPC-H benchmark queries to generate Spark jobs for our simulator.  The original definition of the queries is thanks to:
> TPC-H. 2018. The TPC-H Benchmarks. https://www.tpc.org/tpch/

To use these benchmarks with Spark's built in `spark-submit` utility, we leverage an implementation that uses the DataFrames API that can be found at the following GitHub repository:
> [`ssavvides/tpch-spark`: TPC-H queries in Apache Spark SQL using native DataFrames API](https://github.com/ssavvides/tpch-spark)

**Apache Spark source code:**

In our prototype implementation, we make modifications to the Apache Spark source code to enable communication with our scheduling service.  The original paper is:
> Matei Zaharia, Reynold S. Xin, Patrick Wendell, Tathagata Das, Michael Armbrust, Ankur Dave, Xiangrui Meng, Josh Rosen, Shivaram Venkataraman, Michael J. Franklin, Ali Ghodsi, Joseph Gonzalez, Scott Shenker, and Ion Stoica. 2016. Apache Spark: a unified engine for big data processing. Commun. ACM 59, 11 (November 2016), 56–65. https://doi.org/10.1145/2934664

The current source code can be found at:
> [`apache/spark`: Apache Spark - A unified analytics engine for large-scale data processing](https://github.com/apache/spark)

At the time of implementation, the current version of Spark was 3.5.3.  

**Kubernetes `scheduler-plugins` source code:**

In our prototype implementation, we leverage Kubernetes' scheduler plugins to enable our scheduling service to make scheduling decisions.  The repository is:
> [`kubernetes-sigs/scheduler-plugins`: Kubernetes Scheduler Plugins](https://github.com/kubernetes-sigs/scheduler-plugins)


