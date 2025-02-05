# Carbon-Aware Scheduling for Data Processing
## Prototype Code 
### Experiment Driver

This folder contains scripts and supporting files to run experiments on the Spark cluster using different scheduling models. The experiments simulate workload submission under four carbon-aware scheduling strategies (`PCAPS`, Decima, default behavior, and `CAP` on top of the default behavior), measuring the resulting (simulated) carbon footprint.

## Files

- **`commands.py`**  
  Provides command templates for submitting Spark jobs (using Spark's built-in `spark-submit` utility) for various workloads (e.g., TPC-H queries, Alibaba jobs).  Sets parameters including the Spark docker image, and executor limits.  *Note that these commands ascribe and assume a specific location for each job submission script, which may differ in your environment.*

- **`run-experiment.py`**  
  Orchestrates a series of experiments for each scheduling model. Each experiment, it:
  - Randomizes the start timestamp (in the historical carbon trace) within a given range.
  - Starts necessary backend services (e.g., scheduling service, carbon intensity API, CAP agent for carbon-aware provisioning).
  - Submits jobs using the chosen model.
  - Waits for the experiment to finish and cleans up outstanding processes and pods in the `spark-ns` namespace.
  It accepts command-line parameters to control the number of jobs, submission rate, job type, and other experiment-specific parameters.

- **`submit-measure-jobs.py`**  
  Submits individual Spark jobs to the cluster and tracks the job execution parameters for a single trial.
  - Logs job start time and duration.
  - Calculates carbon intensity using the provided carbon trace.
  - Cleans up completed pods and writes experiment results to a CSV.
  This script is driven by parameters such as the number of jobs, model name, job type, and submission rate.

## Usage
Use `run-experiment.py` to execute a complete experiment run across all models. For example:

```bash
python3 run-experiment.py --num-jobs 100 --carbon-trace sample-carbon-trace.csv --job-type tpch --num-to-avg 5 --submission-rate 2
```

This will run 5 independent experiments across all defined schedulers (default, `PCAPS`, Decima, `CAP` on default), submitting TPC-H jobs at a Poisson arrival rate of 2 jobs per minute and logging the results.
