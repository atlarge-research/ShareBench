import argparse
import os
import subprocess
import time
import yaml
import misc.spark as spark
from misc.s3 import cp_if_exists

CONFIG = 'config.yaml'

def main():
    with open(CONFIG, 'r') as file:
        config = yaml.safe_load(file)

    parser = argparse.ArgumentParser(description="Collect query stats.")
    parser.add_argument("num_executors", type=int, help="Number of executors to use.")
    parser.add_argument("num_runs", type=int, help="Number of runs for each query")
    parser.add_argument("--query", default="all", help="Name of query to collect data for")
    parser.add_argument("--range", default="all", help="Range to collect data for")
    parser.add_argument("--add_conf", default="", help="Additional Spark configuration")
    parser.add_argument("--name", default=None, help="Custom bucket and directory name to use for storage")
    args = parser.parse_args()

    collect_query_stats(config, args.num_executors, args.query, args.range, args.num_runs, args.name, args.add_conf)

def collect_query_stats(config, num_executors, query, range, num_runs, custom_name=None, add_conf=""):

    conf = [
        f"spark.executor.memory={config['spark']['memory']}g",
        f"spark.executor.instances={num_executors}",
        f"spark.kubernetes.executor.podTemplateFile={config['templates']['targets']['executor_pod_simple']['dst']}",
    ]

    name = num_executors if custom_name is None else custom_name
    bucket = f"{config['buckets']['query_stats']}/{name}"

    mode = "query_stats"
    args = f"{query} {range} {num_runs} {bucket}"

    command = spark.get_submit_command(config, mode, args, conf, add_conf)
    print(command)
    subprocess.run(command, check=True, shell=True)

    dir = f"./{config['dirs']['data']}/{config['subdirs']['data']['query_stats']}/{name}"
    if query != 'all':
        bucket = f"{bucket}/{query}.csv"
    else:
        bucket = f"{bucket}/"

    cp_if_exists(config, bucket, dir, "Query Stats")

if __name__ == "__main__":
    main()
