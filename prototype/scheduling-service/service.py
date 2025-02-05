# service.py is a Flask server that communicates with modified versions of Spark and Kubernetes to gather information
# about the cluster, run scheduling logic, and send scheduling decisions back.
# This script is intended to run on the control plane node of a Kubernetes cluster.

from flask import Flask
from flask import request, jsonify
import json
import os
import threading
import subprocess
import requests
import random
import time
import model_plugin
import pandas as pd
import datetime
import argparse
import pytz

utc = pytz.UTC

app = Flask(__name__)

# global registry of application IDs, jobs, and stages
app_registry = {}
driver_ports = {}
driver_ports_proc = {}
jobs = {}
source_job = "not set"
num_source_exec = 0
use_exec = 1
exec_commit = []
frontier_nodes = {}
moving_executors = 0
delayed_thread = None

# constants
MAX_EXECS = 100

parser = argparse.ArgumentParser(description='Run Flask driver server.')
parser.add_argument('--model-name', type=str, default="default", help='Scheduler name')
parser.add_argument('--carbon-trace', type=str, default='sample-carbon-trace.csv', help='Data file for carbon intensity')
parser.add_argument('--initial-date', type=str, default='2021-01-31T22:00:00', help='Initial date for carbon intensity data')
args = parser.parse_args()

MODEL_NAME = args.model_name
data_file_path = args.carbon_trace
INITIAL_DATETIME = datetime.datetime.fromisoformat(args.initial_date)
ACTUAL_DATETIME = datetime.datetime.now()

# global registry of model outputs
sorted_app_id = {}
sorted_stage_prob = {}

# Load the carbon intensity data
try:
    carbon_data = pd.read_csv(data_file_path)
except FileNotFoundError:
    print(f"Carbon intensity data file not found: {data_file_path}")
    exit(1)
carbon_data['datetime'] = pd.to_datetime(carbon_data['datetime'])  # Ensure timestamps are datetime objects
# make the datetime column the index
carbon_data.set_index('datetime', inplace=True)

# resets all variables upon request (e.g., between experiments)
@app.route('/reset', methods=['POST'])
def reset():
    global MODEL_NAME, app_registry, jobs, driver_ports, driver_ports_proc, driver_pods, executor_pods

    # Get the new model name from the request body
    data = request.get_json()
    new_model_name = data.get('model_name')

    # Check if the new model name is valid
    valid_model_names = ["default", "cap", "danish", "decima"]
    if new_model_name not in valid_model_names:
        return jsonify({"error": "Invalid model name."}), 400

    # Reset the global variables
    app_registry = {}
    driver_ports = {}
    driver_ports_proc = {}
    jobs = {}
    source_job = "not set"
    num_source_exec = 0
    exec_commit = []
    frontier_nodes = {}
    moving_executors = 0
    delayed_thread = None

    # Update the MODEL_NAME and initialize the model plugin
    MODEL_NAME = new_model_name
    model_plugin.init(MODEL_NAME)
    ACTUAL_DATETIME = datetime.datetime.now()

    return jsonify({"message": "Environment reset successfully.", "model_name": MODEL_NAME}), 200

# purges any k8s port forwarding processes that may still exist
@app.route('/purge_proc', methods=['GET'])
def purge_proc():
    global driver_ports_proc
    for proc in driver_ports_proc.values():
        proc.kill()
    return jsonify({"message": "Port-forwarding processes purged successfully."}), 200

# register job is called by new Spark jobs to register a job with the scheduling service and communicate DAG dependency information.
@app.route('/registerJob', methods=['POST'])
def register_job():
    global app_registry
    data = request.get_json()
    app_id = data['appId']
    job_data = data['data']
    
    if app_id not in app_registry:
        app_registry[app_id] = {"exec_limit": 1}
    
    # Parse job and stage dependencies
    parts = job_data.split(", Stage ")
    job_id = int(parts[0].split(" ")[1])
    stage_parts = parts[1].split(" depends on stages: ")
    stage_id = int(stage_parts[0])
    dependencies = [int(dep) for dep in stage_parts[1].split(", ") if dep]
    
    if job_id not in app_registry[app_id].keys():
        app_registry[app_id][job_id] = {}
    
    if stage_id not in app_registry[app_id][job_id].keys():
        app_registry[app_id][job_id][stage_id] = {"dependencies": dependencies, "status": "PENDING"}
    else:
        app_registry[app_id][job_id][stage_id]["dependencies"] = dependencies
    return "Thanks!"

# gathers information about pods that Kubernetes can prioritize for resources, which correspond to different Spark jobs.
@app.route('/pods', methods=['POST'])
def pods():
    global jobs, source_job, driver_ports, driver_ports_proc, num_source_exec, use_exec, exec_commit, app_registry, frontier_nodes, moving_executors
    global sorted_app_id, sorted_stage_prob, delayed_thread
    if request.data == b'null':
        print("empty request")
        return jsonify({'response': 'empty'})
    
    # extract data from request
    data = request.get_json()
    pod_names = [pod['name'] for pod in data]
    driver_pods = [pod for pod in data if 'driver' in pod['name']]
    executor_pods = [pod for pod in data if 'exec' in pod['name']]
    active_execs = 0
    
    # Start port-forwarding for any new driver pods to communicate with Spark
    port = 4040
    for driver_pod in driver_pods:
        driver_name = driver_pod['name']
        if driver_name not in driver_ports:
            while port in driver_ports.values():
                port += 1
            driver_ports[driver_name] = port
            port_forward_cmd = f"kubectl port-forward {driver_name} {port}:4040 -n spark-ns"
            proc = subprocess.Popen(port_forward_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            driver_ports_proc[driver_name] = proc

    # each driver pod is a single job.
    # each driver pod also owns some executor pods, whose status may be "Running" or "Pending":
    prev_exec = {}
    exec_commit = []

    for driver_pod in driver_pods:
        driver_name = driver_pod['name']
        prev_exec[driver_name] = jobs[driver_name]["active_executors"] if driver_name in jobs else 0
        jobs[driver_name] = {"executors": [], "active_executors": 0}
    
    for executor_pod in executor_pods:
        driver_name = executor_pod['ownerRefs'][0]['name']
        jobs[driver_name]["executors"].append(executor_pod['name'])
        if executor_pod['status'] == "Running":
            jobs[driver_name]["active_executors"] += 1
            active_execs += 1
            exec_commit.append((executor_pod['name'], driver_name))
    moving_executors = MAX_EXECS - active_execs
    
    # find source_job and num_source_exec if they have changed
    for driver_pod in driver_pods:
        driver_name = driver_pod['name']

        # if the number of active executors went down, this is the source job
        if prev_exec[driver_name] > jobs[driver_name]["active_executors"]:
            source_job = driver_name
            num_source_exec = jobs[source_job]["active_executors"]
        
        if len(jobs[driver_name]["executors"]) == 0:
            # flush port-forwarding for driver pods with no active or pending executors
            driver_ports.pop(driver_name)
            driver_ports_proc[driver_name].terminate()
            driver_ports_proc.pop(driver_name)
            continue

        port = driver_ports[driver_name]
        
        # Query the /applications endpoint
        try:
            update_from_API(driver_name, port)
        except:
            # retry if the port is slow to open
            time.sleep(1)
            try:
                update_from_API(driver_name, port)  
            except:
                continue

    # set carbon intensity based on the trace
    carbon_intensity, L, U = get_carbon_intensity()
    print(f"Carbon intensity: {carbon_intensity} gCO2/kWh")
    print(f"Lower bound: {L} gCO2/kWh")
    print(f"Upper bound: {U} gCO2/kWh")

    # call model_plugin to get new scheduling actions.
    print("getting actions from {}".format(MODEL_NAME))
    action, carbonAware, sorted_app_id, sorted_stage_prob, exec_decision = model_plugin.get_actions(app_registry, source_job, frontier_nodes, exec_commit, moving_executors, num_source_exec, carbon_intensity, L, U)     

    # update use_exec
    if exec_decision > 0:
        use_exec = exec_decision
    # if exec_decision is 0, the scheduler is PCAPS and we are going to defer scheduling.

    # use new actions to prioritize jobs in queue
    sorted_pod_names = []

    for app_id in sorted_app_id.keys():
        # look at pods in pod names that belong to this app_id, place them in sorted_pod_names
        driver_pod = app_registry[app_id]["driver_pod"]
        executors = jobs[driver_pod]["executors"]
        for pod_name in pod_names:
            if pod_name in executors:
                sorted_pod_names.append(pod_name)

    # if there are any remaining pods, add them to the end of sorted_pod_names
    for pod_name in pod_names:
        if pod_name not in sorted_pod_names:
            sorted_pod_names.append(pod_name)

    # if carbonAware action, add "PAUSE" to the beginning of sorted_pod_names -- this will trigger the Kubernetes
    # scheduler to stop scheduling new pods until the scheduler pod is restarted (delayed_pod_delete).
    # note that carbonAware is only triggered by PCAPS (see model_plugin.py)
    if carbonAware and active_execs > 1:
        print("pausing for carbon intensity")

        sorted_pod_names.insert(0, "PAUSE")
        # Check if there is no existing thread or if the existing thread is not alive
        if delayed_thread is None or not delayed_thread.is_alive():
            # Start a new thread to run the delayed command, with a delay of 60 seconds
            delayed_thread = threading.Thread(target=delayed_pod_delete, args=(60))
            delayed_thread.start()
    
    # if the model is not Decima or PCAPS, we return "nothing" so that Kubernetes follows its usual behavior.
    if MODEL_NAME == "default" or "cap":
        sorted_pod_names = ["nothing"]

    return jsonify(sorted_pod_names)


# update the app_registry and frontier_nodes using the Spark REST API -- this is context Decima uses as input
def update_from_API(driver_name, port):
    global app_registry, frontier_nodes
    response = requests.get(f"http://localhost:{port}/api/v1/applications/")
    # get as JSON
    app = response.json()[0]['id']
    if app not in app_registry.keys():
        app_registry[app] = {"exec_limit": 1}
    if "driver_pod" not in app_registry[app].keys():
        app_registry[app]["driver_pod"] = driver_name

    # query for job information
    response = requests.get(f"http://localhost:{port}/api/v1/applications/{app}/jobs/")
    jobs = response.json()

    # query for stage information
    response = requests.get(f"http://localhost:{port}/api/v1/applications/{app}/stages/")
    stages = response.json()

    # query for executor information (if not set)
    if app_registry[app]["exec_limit"] == 1:
        response = requests.get(f"http://localhost:{port}/api/v1/applications/{app}/environment/")
        properties = response.json()["sparkProperties"]
        # set exec_limit to the number of executors (spark.executor.instances)
        filteredProps = [prop for prop in properties if prop[0] == "spark.executor.instances"]
        if len(filteredProps) > 0:
            app_registry[app]["exec_limit"] = int(filteredProps[0][1])


    for job in jobs:
        job_id = job['jobId']
        if job_id not in app_registry[app]:
            app_registry[app][job_id] = {}
        for stage_id in job['stageIds']:
            if stage_id not in app_registry[app][job_id]:
                app_registry[app][job_id][stage_id] = {"dependencies": [], "status": "PENDING"}
    
    for stage in stages:
        stage_id = stage['stageId']
        job_id = next(job['jobId'] for job in jobs if stage_id in job['stageIds'])
        app_registry[app][job_id][stage_id]["status"] = stage['status']
        if stage['status'] != "SKIPPED":
            app_registry[app][job_id][stage_id]["num_tasks"] = stage['numTasks']
            app_registry[app][job_id][stage_id]["num_finished_tasks"] = stage['numCompleteTasks']
        else:
            app_registry[app][job_id][stage_id]["num_tasks"] = 1
            app_registry[app][job_id][stage_id]["num_finished_tasks"] = 1
    
    # Compute and refresh frontier_nodes
    frontier_nodes[app] = {}
    for job_id in app_registry[app].keys():
        if job_id == "driver_pod" or job_id == "exec_limit":
            continue
        for stage_id in app_registry[app][job_id].keys():
            stage_info = app_registry[app][job_id][stage_id]
            if (stage_info["status"] == "PENDING" or stage_info["status"] == "ACTIVE") and all(
                app_registry[app][job_id][dep_id]["status"] == "COMPLETE" for dep_id in stage_info["dependencies"]
            ):
                if job_id not in frontier_nodes[app]:
                    frontier_nodes[app][job_id] = []
                frontier_nodes[app][job_id].append(stage_id)
    return 

# gathers information about tasks that Spark can work on, and reorders Spark's task queue based on inference
# from either the Decima or PCAPS schedulers.
@app.route('/task', methods=['POST'])
def task():
    global sorted_stage_prob
    queue = request.get_json()

    # first extract the app id if the queue is not empty
    if queue:
        app_id = queue[0]['taskSet']['tasks'][0]['appId']

        if app_id in sorted_stage_prob.keys():
            # get the probabilities for the app id
            probs = sorted_stage_prob[app_id]

            # make the order of the queue match the order in probs, based on stageId (probs.keys())
            # doing a for loop to better handle the case where probs does not contain the stages in the queue
            for stage_id in reversed(probs.keys()):
                # if the stage is in the queue, move it to the front
                for i, task in enumerate(queue):
                    if task['taskSet']['tasks'][0]['stageId'] == stage_id:
                        queue.insert(0, queue.pop(i))

    # for either default or CAP, we return the queue in the same order it is given to us (FIFO)
    if MODEL_NAME == "default" or MODEL_NAME == "cap":
        queue = request.get_json()
    
    # Return the manipulated queue list and the current use_exec decision (parallelism limit) as a response
    response = {
        "queue": queue,
        "use_exec": use_exec
    }
    
    return jsonify(response)

# Restarts the kube-scheduler pod
def delayed_pod_delete(delay):
    time.sleep(delay)
    subprocess.run(['kubectl', 'delete', 'pod', 'kube-scheduler-node-0', '-n', 'kube-system'])

def get_carbon_intensity():
    global ACTUAL_DATETIME, INITIAL_DATETIME
    # Calculate the time delta
    current_datetime = datetime.datetime.now()
    time_delta = current_datetime - ACTUAL_DATETIME

    # actual real time sped up by a factor of 60 (1 minute in real time = 1 hour in simulation time)
    elapsed_hours = int(time_delta.total_seconds() // 60)

    # Determine the corresponding row in the carbon intensity data
    carbon_time = (INITIAL_DATETIME + datetime.timedelta(hours=elapsed_hours)).replace(tzinfo=utc)
    # if the carbon time is beyond the last time in the data, reset the ACTUAL_DATETIME (so that we loop back to the beginning of the data)
    if carbon_time > carbon_data.index[-1]:
        ACTUAL_DATETIME = current_datetime
        INITIAL_DATETIME = carbon_data.index[0]
        carbon_time = INITIAL_DATETIME
        elapsed_hours = 0
    rounded_time = carbon_time.replace(minute=0, second=0, microsecond=0)  # Round down to the nearest hour
    future_time = rounded_time + datetime.timedelta(hours=48)
    # convert to iso format
    rounded_time = rounded_time.isoformat()
    future_time = future_time.isoformat()

    # Retrieve the intensity value
    # note that rounded_time is a datetime object so we can index the carbon_data DataFrame with it
    try:
        row = carbon_data.loc[rounded_time]
    except KeyError:
        print(f"Carbon intensity data not available for the requested time: {rounded_time}")
        return jsonify({"error": "Carbon intensity data not available for the requested time."}), 404
    carbon_intensity = row['carbon_intensity_avg']
    
    # Also retrieve the upper and lower bounds on future carbon intensities for up to 48 hours after the current (rounded) time
    future_carbon_intensity = carbon_data.loc[rounded_time:future_time]
    lower_bound = future_carbon_intensity['carbon_intensity_avg'].min()
    upper_bound = future_carbon_intensity['carbon_intensity_avg'].max()

    return carbon_intensity, lower_bound, upper_bound


if __name__ == '__main__':
    try:
        # initialize the model_plugin to run inference
        model_plugin.init(MODEL_NAME)
        # open communication at a default IP and location
        app.run(host='192.168.1.10', port=14040)
    except:
        # kill any remaining port-forwarding processes
        for proc in driver_ports_proc.values():
            proc.kill()
        print("Exiting...")
