# run-experiment.py runs one experiment for each model on the cluster, using the specified parameters.
# this script is intended to be run on the control plane node of the cluster.

import subprocess
import argparse
import time
import random
import os
from datetime import datetime, timedelta

# command line arguments 
parser = argparse.ArgumentParser(description='Run experiments on Spark.')
parser.add_argument('--num-jobs', type=int, default=100, help='Number of jobs to submit')
parser.add_argument('--start', type=int, default=0, help='Starting index for result CSV files')
parser.add_argument('--carbon-trace', type=str, default="sample-carbon-trace.csv", help='Carbon trace to use')
parser.add_argument('--job-type', type=str, default="tpch", help='Type of job to run')
parser.add_argument('--num-to-avg', type=int, default=1, help='Number of times to average each experiment')
parser.add_argument('--custom-path', type=str, default="", help='Custom path for saving results (default blank)')
parser.add_argument('--submission-rate', type=float, default=2, help='Job submission rate for Poisson process (default is \lambda = 2, i.e., 2 jobs per minute (min = simulated hour))')
parser.add_argument('--lower-bound-cap', type=int, default=20, help='B parameter for CAP agent')
args = parser.parse_args()

# generate random start time
# Define the start and end dates
start_date = datetime(2021, 1, 1, 0, 0, 0)
end_date = datetime(2021, 6, 31, 23, 0, 0)

# Generate a random timestamp between start_date and end_date
random_timestamp = start_date + timedelta(seconds=random.randint(0, int((end_date - start_date).total_seconds())))

# Round the timestamp to the nearest hour
rounded_timestamp = random_timestamp.replace(minute=0, second=0, microsecond=0)
if random_timestamp.minute >= 30:
    rounded_timestamp += timedelta(hours=1)
print(rounded_timestamp.isoformat())

# check to make sure carbon trace file exists
try:
    with open(args.carbon_trace, "r") as f:
        pass
except FileNotFoundError:
    print(f"Carbon trace file {args.carbon_trace} not found.")
    exit(1)

MODELS = ["default", "pcaps", "decima", "cap"]
processes = []

# create folders results/{MODEL_NAME} for each model if they don't already exist, use os
if not os.path.exists(f"results"):
    os.makedirs(f"results")
if not os.path.exists(f"logs"):
    os.makedirs(f"logs")
for model in MODELS:
    if not os.path.exists(f"results/{model}"):
        os.makedirs(f"results/{model}")

# runs a single experiment with the given rounded timestamp in the trace and all four test schemes.
def run_experiment(model_name, i):
    global processes
    num_jobs = args.num_jobs
    print(f"Running {num_jobs} jobs with model {model_name}")

    # note that the 

    # first, start the flask driver server
    print("Starting flask server for scheduling service...")
    flask_log = open(f"logs/flask_{model_name}.log", "w")
    processes.append(subprocess.Popen(
        ["python3", "/home/cc/scheduling-service/service.py", "--model-name", model_name, "--carbon-trace", args.carbon_trace, "--initial-date", rounded_timestamp.isoformat() ],
        stdout=flask_log,
        stderr=flask_log
    ))

    # start the carbon intensity server if model_name == CAP
    if model_name == "cap":
        print("Starting carbon intensity server...")
        carbon_log = open("logs/carbon_intensity_server.log", "w")
        processes.append(subprocess.Popen(
            ["python3", "/home/cc/carbon-intensity-api-sim/carbonServer.py", "--carbon-trace", args.carbon_trace],
            stdout=carbon_log,
            stderr=carbon_log
        ))

        # wait a few seconds for the carbon intensity server to start
        time.sleep(5)

        print("Starting CAP agent...")
        cap_log = open("logs/cap_agent.log", "w")
        processes.append(subprocess.Popen(
            ["python3", "/home/cc/cap-k8s/cap.py", "--namespace", "spark-ns", "--res-quota-path", "/home/cc/cap-k8s/resource_quota.yaml", "--api-domain", "127.0.0.1:6066", "--min-execs", str(args.lower_bound_cap), "--max-execs", "100", "--interval", "60", "--initial-date", rounded_timestamp.isoformat()],
            stdout=cap_log,
            stderr=cap_log
        ))
    
    # run the experiment
    print("Running experiment...")
    exp_log = open(f"logs/experiment_{model_name}.log", "w")
    exp = subprocess.Popen(
        ["python3", 
         "/home/cc/experiment-driver/submit-measure-jobs.py", 
         "--num-jobs", str(num_jobs), 
         "--model-name", model_name, 
         "--carbon-trace", args.carbon_trace, 
         "--tag", f"{i}", 
         "--job-type", args.job_type, 
         "--initial-date", rounded_timestamp.isoformat(), 
         "--custom-path", args.custom_path,
         "--submission-rate", str(args.submission_rate)],
    )

    # wait for the experiment to finish
    exp.wait()

    # send a GET request to the flask driver at /purge_proc to get rid of any port forwarding still hanging around
    print("Purging any outstanding port-forwarding processes...")
    subprocess.run(["curl", "http://192.168.1.10:14040/purge_proc"], check=True)

    # once it is done, clean up all the processes
    for p in processes:
        print("Stopping all processes...")
        p.kill()
    
    return
    
if __name__ == "__main__":
    num_to_avg = args.num_to_avg
    start = args.start
    for i in range(start, num_to_avg+start):
        # shuffle the order of the models
        random.shuffle(MODELS)
        for model in MODELS:
            run_experiment(model, i)
            # add a delay of 10 seconds between experiments
            print("Sleeping for 10 seconds...")
            time.sleep(10)
            # just to be sure, clear the namespace of all pods again
            print("Deleting all pods in the spark-ns namespace...")
            subprocess.run(["kubectl", "delete", "pods", "--all", "-n", "spark-ns"], check=True)
    print("All experiments completed.")
    exit(0)


