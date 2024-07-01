import argparse
import os
import subprocess
import time
from datetime import datetime
import yaml
import select
from kube_configure_nodes import kube_configure_nodes

CONFIG = 'config.yaml'
SPARK_SUBMIT = 'bin/spark-submit'
DIR_TMP = '.tmp'

def create_pod_templates(config, num_apps):
    
    dir_tmp_pod_templates = f"{config['dirs']['pod_templates']}/{DIR_TMP}"
    path_template_executor = config['templates']['targets']['executor_pod']['dst']

    # Create pod templates
    os.makedirs(dir_tmp_pod_templates, exist_ok=True)

    with open(path_template_executor) as file:
        template = file.read()
    for i in range(num_apps):
        with open(f"{dir_tmp_pod_templates}/executor_{i}.yaml", 'w') as file:
            file.write(template.replace("$(SPARK_APP_ID)", str(i)))

def submit_spark_apps(config, workload, num_apps, start_time, add_conf):

    # Spark submit command and flags
    spark_submit = f"{config['dirs']['spark']}/{SPARK_SUBMIT}"
    spark_flags = f"\
        --class {config['scala']['class']} \
        --properties-file {config['templates']['targets']['spark']['dst']} \
        --conf spark.kubernetes.driver.podTemplateFile=./{config['dirs']['pod_templates']}/driver.yaml \
        --deploy-mode cluster\
    "
    spark_flags = ' '.join(spark_flags.split())
    spark_jar = f"local:///opt/{config['general']['name']}/{config['general']['name']}_{config['scala']['version_short']}-1.0.jar"
    spark_mode = "workload"
    dir_tmp_pod_templates = f"{config['dirs']['pod_templates']}/{DIR_TMP}"

    # Check if SPARK_SUBMIT file exists
    if not os.path.isfile(spark_submit):
        raise FileNotFoundError(f"Error: \'{spark_submit}\' not found. Are you in the root directory of the project?")

    processes = []

    for i in range(num_apps):
        conf_executor_template = f"--conf spark.kubernetes.executor.podTemplateFile={dir_tmp_pod_templates}/executor_{i}.yaml"
        command = f"{spark_submit} {spark_flags} {conf_executor_template} {add_conf} {spark_jar} {spark_mode} {workload} {i} {start_time}"
        print(command)
        processes.append(subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE))

    return processes

def follow_processes(processes):
    return_codes = []

    while processes:
        for proc in processes:
            rc = proc.poll()
            if rc is not None:
                return_codes.append(rc)
                processes.remove(proc)
                continue
            
            streams, _, _ = select.select([proc.stdout, proc.stderr], [], [], 0.01)
            for stream in streams:
                output = stream.readline()
                if output: 
                    print(output.decode(), end='')
                
    return return_codes

def save_pod_logs(pod_names, dir_logs_session):
    for pod in pod_names:
        with open(f"{dir_logs_session}/{pod}.txt", 'w') as f:
            subprocess.run(["kubectl", "logs", pod], stdout=f)

def s3_cp_if_exists(bucket, dst, name):
    if subprocess.run(["./bin/mc", "find", bucket], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0:
        subprocess.run(["./bin/mc", "cp", "-r", bucket, dst])
        print(f"Success: {name} saved to {dst}!")
    else:
        print(f"Warning: Bucket {bucket} does not exist in the S3 storage. Something must have gone wrong with the collection in the applications.")

def run_workload(config, workload, num_apps, start_delay, add_conf=""):

    create_pod_templates(config, num_apps)
    kube_configure_nodes(config['kubernetes']['nodes'], num_apps)

    # Record start time
    script_start_time = int(time.time())
    start_time = script_start_time + args.start_delay
    session_id = f"{workload}/{start_time}"

    processes = submit_spark_apps(config, workload, num_apps, start_time, add_conf)
    return_codes = follow_processes(processes)

    # # Example of saving pod logs (adjust as needed)
    # pod_names = ["pod1", "pod2"]  # Placeholder for actual pod names
    # dir_logs_session = f"./logs/{session_id}"
    # os.makedirs(dir_logs_session, exist_ok=True)
    # save_pod_logs(pod_names, dir_logs_session)

    # Example of copying from S3 (adjust as needed)
    # s3_bucket_traces_session = f"sparkbench/data/workload-traces/{session_id}"
    # s3_cp_if_exists(s3_bucket_traces_session, f"./traces/{session_id}", "Workload traces")

    print("Workload run finished!")

if __name__ == "__main__":
    with open(CONFIG, 'r') as file:
        config = yaml.safe_load(file)

    parser = argparse.ArgumentParser(description="Submit Spark workloads.")
    parser.add_argument("num_apps", type=int, help="Number of applications to submit")
    parser.add_argument("workload", help="Workload file path")
    parser.add_argument("start_delay", type=int, help="Delay before starting the workload")
    parser.add_argument("--add_conf", default="", help="Additional Spark configuration")
    args = parser.parse_args()
    run_workload(config, args.workload, args.num_apps, args.start_delay, args.add_conf)