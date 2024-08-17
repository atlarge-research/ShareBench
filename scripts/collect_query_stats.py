import argparse
import os
import subprocess
import time
import yaml
from misc.filenames import get_spark_path, get_spark_jar_path
from misc.s3 import cp_if_exists

CONFIG = 'config.yaml'
SPARK_SUBMIT = 'bin/spark-submit'

def collect_query_stats(config, num_executors, query, range, num_runs, custom_name=None, add_conf=""):

    conf = []
    conf.append(f"spark.executor.memory={config['spark']['memory']}g")
    conf.append(f"spark.executor.instances={num_executors}")

    name = num_executors if custom_name is None else custom_name
    bucket = f"{config['buckets']['query_stats']}/{name}"

    spark_submit = f"{get_spark_path(config)}/{SPARK_SUBMIT}"
    spark_flags = f"""
            --class {config['scala']['class']}
            --properties-file {config['templates']['targets']['spark']['dst']}
            --conf spark.kubernetes.driver.podTemplateFile={config['templates']['targets']['driver_pod']['dst']}
            --conf spark.kubernetes.executor.podTemplateFile={config['templates']['targets']['executor_pod_simple']['dst']}
            --deploy-mode cluster
        """
    spark_flags = ' '.join(spark_flags.split())
    spark_jar = get_spark_jar_path(config)

    mode = "query_stats"
    args = f"{query} {range} {num_runs} {bucket}"

    command = ' '.join([
        spark_submit,
        spark_flags,
        ' '.join(list(map(lambda s: "--conf " + s, conf))),
        add_conf,
        spark_jar,
        mode,
        args
    ])

    print(command)
    subprocess.run(command, check=True, shell=True)

    dir = f"./{config['dirs']['data']}/{config['subdirs']['data']['query_stats']}/{name}"
    if query != 'all':
        bucket = f"{bucket}/{query}.csv"
    else:
        bucket = f"{bucket}/"
        
    cp_if_exists(config, bucket, dir, "Query Stats")

if __name__ == "__main__":
    with open(CONFIG, 'r') as file:
        config = yaml.safe_load(file)

    parser = argparse.ArgumentParser(description="Collect query stats.")
    # parser.add_argument("num_apps", type=int, help="Number of applications to submit")
    # parser.add_argument("workload", help="Workload file path")
    # parser.add_argument("start_delay", type=int, help="Delay before starting the workload")
    # parser.add_argument('-m', '--mechanism', type=str, default=None, help="Mechansim to use.")
    parser.add_argument("num_executors", type=int, help="Number of executors to use.")
    parser.add_argument("num_runs", type=int, help="Number of runs for each query")
    parser.add_argument("--query", default="all", help="Name of query to collect data for")
    parser.add_argument("--range", default="all", help="Range to collect data for")
    parser.add_argument("--add_conf", default="", help="Additional Spark configuration")
    parser.add_argument("--name", default=None, help="Custom bucket and directory name to use for storage")
    args = parser.parse_args()

    collect_query_stats(config, args.num_executors, args.query, args.range, args.num_runs, args.name, args.add_conf)

    # run_workload(config, args.workload, args.num_apps, args.start_delay, args.add_conf, args.mechanism)