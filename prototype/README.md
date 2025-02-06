# Carbon- and Precedence-Aware Scheduling for Data Processing
## Prototype Code 

Each folder in this directory corresponds to one component of our Spark+Kubernetes implementation.  Throughout this code, we assume these folders are placed in a user's home folder (`~/`) on the control plane node of a Kubernetes cluster.

### Assumed Cluster Details

Below we give an excerpt from our paper that gives basic information about our experimental cluster.  Please consult the paper for more details.
> Our prototype is deployed on an OpenStack cluster running Kubernetes v1.31 and Spark v3.5.3 in Chameleon Cloud. Our testbed consists of 51 `m1.xlarge` virtual machines, each with 8 VCPUs and 16GB of RAM.  One VM is designated as the control plane node, while the remaining 50 are workers, each hosting two executor pods.Our Spark configuration allocates 4 VCPUs and 7GB of RAM to each of the 100 executors.  To avoid a known issue with Spark's dynamic allocation feature that can cause it to hang on Kubernetes, we configure an upper limit of 25 executors that can be allocated to any single job. 

Although we do not give explicit instructions on setting up a Kubernetes cluster in [Chameleon Cloud](https://chameleoncloud.org), we found this resource very helpful -- it provides a Jupyter Notebook to setup a Kubernetes cluster on `KVM@TACC` using Kubespray.
> [Deploy a Kubernetes cluster - Chameleon Cloud](https://chameleoncloud.org/experiment/share/9bae9a8a-68fa-402c-ae51-41431eb78732)

Our code also assumes a Python environment (on the control plane node) with the following packages installed:
- [NumPy](https://numpy.org)
- [SciPy](https://scipy.org)
- [pandas](https://pandas.pydata.org)
- [networkx](https://networkx.org)
- [TensorFlow](https://www.tensorflow.org) for Decima inference
- [tf-slim](https://github.com/google-research/tf-slim)
- [Matplotlib](https://matplotlib.org) for creating plots 
- [seaborn](https://seaborn.pydata.org) for creating plots 
- [requests](https://pypi.org/project/requests/) for making HTTP requests
- [PyYAML](https://pypi.org/project/PyYAML/) for parsing YAML files
- [Flask](https://flask.palletsprojects.com) 

### Directory Structure

- **`carbon-intensity-api-sim/`**  
  Contains a simulated real-time carbon-intensity service. The `carbonServer.py` script runs a Flask API that replays historical carbon intensity traces CSV files for different regions. 

- **`alibaba-spark-jobs/`**  
  This folder contains an Alibaba cluster trace, along with some code to parse the DAG (directed acyclic graph) structure and programmatically generate SparkPy jobs that simulate the execution of these DAGs.  We include 100 SparkPy jobs generated this way as a sample.

- **`cap-k8s/`**  
  Contains the core implementation of the `CAP` carbon-aware wrapper for any carbon-agnostic scheduler.  This code interfaces with the Kubernetes API and the external carbon intensity API to dynamically adjust the resource quota for executors available to Spark on the cluster.

- **`modified-spark/`**  
  A fork of **[apache/spark](https://github.com/apache/spark)** with modifications made to enable the `PCAPS` scheduler.  These modifications make the Spark driver of each application communicate with our scheduling service (detailed below).

- **`scheduler-plugins/`**  
  A fork of **[kubernetes-sigs/scheduler-plugins](https://github.com/kubernetes-sigs/scheduler-plugins)** that implements a plugin for the Kubernetes scheduler pod to enable the `PCAPS` scheduler.  These modifications make the Kubernetes scheduler communicate with our scheduling service (detailed below).

- **`scheduling-service/`**  
  A Flask-based service that communicates with both Spark and Kubernetes to implement [Decima](https://web.mit.edu/decima/) and `PCAPS`.  This service assumes that Spark and Kubernetes are modified according to `modified-spark` and `scheduler-plugins`, and that the control plane node is accessible at address `192.168.1.10` from the other nodes in the cluster.

- **`cluster-config/`**  
  Contains scripts and config files for first-time setup of the cluster, including setting up a namespace, service account, and role binding for the scheduling service.