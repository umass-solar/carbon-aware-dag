# Carbon- and Precedence-Aware Scheduling for Data Processing
## Prototype Code 
### Spark jobs based on Alibaba traces

## Overview
The code in this repository programmatically generates PySpark scripts (`generate.ipynb`)to simulate the execution of DAGs from an Alibaba production cluster trace (`sorted_batch_task.csv`, cited below).  We include ~100 sample scripts in `spark_jobs/` to illustrate how DAG execution is simulated using the PySpark API.

> Alibaba. 2018. Cluster data collected from production clusters in Alibaba for cluster management research. [`alibaba/clusterdata/tree/master/cluster-trace-v2018`](https://github.com/alibaba/clusterdata/tree/master/cluster-trace-v2018)
