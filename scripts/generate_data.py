import argparse
import subprocess
import yaml
import misc.spark as spark
from kube_configure_nodes import kube_configure_nodes
from misc.s3 import cp_if_exists

PATH_CONFIG = 'config.yaml'

def main():
    with open(PATH_CONFIG, "r") as file:
        config = yaml.safe_load(file)
    
    bucket_default = f"s3a://{config['buckets']['tpcds']}"

    parser = argparse.ArgumentParser(description="Run data- and/or metagen to generate and structure the TPC-DS data.")
    parser.add_argument('-m', '--mode', default=None, choices=['data', 'meta'], help=f"Target mode. Either 'data' or 'meta'. If left empty, both will be executed subsequently.")
    parser.add_argument('-d', '--dst', default=None, help=f"Custom address for data storage. Defaults to '{bucket_default}' bucket on S3A (minio).")
    args = parser.parse_args()

    bucket = args.dst if args.dst is not None else bucket_default

    kube_configure_nodes(config['kubernetes']['nodes'], 1)

    if args.mode == None or args.mode == 'data':
        generate_data(config, bucket)
    if args.mode == None or args.mode == 'meta':
        generate_meta(config, bucket)

def generate_data(config, bucket):
    print('Starting data generation.')
    path_dsdgen = f"/opt/{config['general']['name']}/tpcds-bin/"
    command = spark.get_submit_command(config, 'datagen', [bucket, path_dsdgen])
    print(command)
    subprocess.run(command, shell=True, check=True)
    print('Data generation finished. Note that the data is not yet structured (metagen).')
    pass

def generate_meta(config, bucket):
    print('Starting structuring of data (metagen).')
    command = spark.get_submit_command(config, 'metagen', [bucket])
    print(command)
    subprocess.run(command, shell=True, check=True)
    print('Structuring finished.')
    pass

if __name__ == '__main__':
    main()
