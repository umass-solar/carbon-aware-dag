# Carbon- and Precedence-Aware Scheduling for Data Processing
## Prototype Code 
### CAP (Carbon-Aware Provisioning) module for Kubernetes and Spark

## Overview
This folder contains a script (`cap.py`) and a resource quota definition (`resource_quota.yaml`) that dynamically adjust Spark’s CPU and memory limits for executors based on the current carbon intensity. 

## Files
- **`cap.py`**  
  A Python script that:  
  1. Periodically queries the carbon-intensity API for current carbon intensity data.  
  2. Derives an allowable number of executor pods using a threshold function.  
  3. Updates the resource quota in Kubernetes to constrain the total CPU and memory usage accordingly.

- **`resource_quota.yaml`**  
  A Kubernetes `ResourceQuota` definition that sets an initial upper bound on CPU and memory for Spark jobs. The `cap.py` script overwrites this file’s CPU and memory values to dynamically manage resource consumption.

## Usage
Execute `python3 cap.py --help` to see available parameters.  The typical usage is:

```bash
python3 cap.py \
--namespace spark-ns \
--res-quota-path /home/cc/cap-k8s/resource_quota.yaml \
--api-domain 127.0.0.1:6066 \
--min-execs 20 \
--max-execs 100 \
--interval 60 \
--initial-date 2021-01-31T18:00:00
```

By default, the script runs continuously. Use `--run-once True` to apply a one-time update and exit.