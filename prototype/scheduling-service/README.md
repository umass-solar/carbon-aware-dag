# Carbon- and Precedence-Aware Scheduling for Data Processing
## Prototype Code 
### Scheduling Service

This folder implements the scheduling service that bridges modified versions of Spark and Kubernetes to support carbon-aware scheduling. It orchestrates job scheduling, executor management, and integrates with an ML agent ([Decima](https://web.mit.edu/decima/)).

## Files

The scheduling service is built around a Flask server and several supporting modules:

- **Scheduling Service:**  
  The Flask-based server (`service.py`) exposes REST endpoints for:
  - Resetting the environment.
  - Registering Spark jobs.
  - Receiving pod and task updates.
  
  The service uses these endpoints to communicate with Spark and Kubernetes and send scheduling decisions back to the cluster.  Note that this code starts the server on `192.168.1.10:14040` within the cluster's internal network -- this address should be reachable from any node in the cluster, and `192.168.1.10` should correspond to the control plane node.  Otherwise, changes must be made to both Spark (`modified-spark/`) and Kubernetes (`scheduler-plugins/`) to point to the correct address.
  
- **Spark Environment Simulation:**  
  The `spark_env` environment and associated files are carried over from the simulator (see `simulator/`) to run inference through Decima.

- **Model Integration:**  
  The `model_plugin.py` file bridges the scheduling service with Decima inference by translating the cluster state into simulator-compatible representations.

