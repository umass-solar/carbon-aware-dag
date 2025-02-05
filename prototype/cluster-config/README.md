# Carbon-Aware Scheduling for Data Processing
## Prototype Code 
### Cluster Config

This directory contains YAML manifests, configuration templates, and setup scripts for configuring a Kubernetes cluster using the same config as our prototype. Below is a summary of each file and instructions on how to use them:

## Files

- **`resource_quota_template.yaml`**  
  Defines an initial Kubernetes `ResourceQuota` object that limits the CPU and memory usage in the `spark-ns` namespace (this is used by `CAP`). Adjust the CPU and memory values here to match the **maximum number of Spark executors** in a cluster.

- **`kube-scheduler.yaml`**  
  A pod spec for the Kubernetes scheduler that references a custom scheduler configuration file (`sched-cc.yaml`). Modifies the default scheduler to include a custom scheduling plugin -- be sure to paste the tag of a Docker scheduler image compiled from `prototype/scheduler-plugins`.

- **`sched-cc.yaml`**  
  A `KubeSchedulerConfiguration` file that enables the necessary plugin for `PCAPS` in the `spark-ns` namespace. 

- **`setup_kube_in_docker.sh`**  
  A convenience script to copy the modified scheduler manifests into the cluster, apply the custom scheduler manifests, and restart the kubelet service. It also applies CRDs for the scheduler plugin the `kube-scheduler-manifests` folder.  
  - Be sure to paste the tag of a Docker controller image compiled from `prototype/scheduler-plugins` into the file `kube-scheduler-manifests/all-in-one.yaml`.
  - Run this from the control plane node.

- **`setup_spark.sh`**  
  Creates the `spark-ns` namespace, sets up a `ServiceAccount`, and binds the `spark` service account to an `edit` cluster role. It also applies the `resource_quota_template.yaml` in `spark-ns`.  
  - Ensure that you edit `resource_quota_template.yaml` to suit your cluster resources.
  - Run this from the control plane node.