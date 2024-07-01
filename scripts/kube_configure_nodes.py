import argparse
import subprocess
import yaml

# TODO: add option to disable nodes?

CONFIG = 'config.yaml'

def kube_unmark_node(node, label, type="taint"):
    cmd_check_mark = f"kubectl describe node {node} | grep -q '{label}'"
    cmd_unmark = f"kubectl {type} node {node} {label}-"

    try:
        subprocess.check_call(cmd_check_mark, shell=True)
        subprocess.run(cmd_unmark, shell=True)
    except subprocess.CalledProcessError:
        pass

def kube_configure_nodes(num_nodes, num_drivers):

    for node in [f"cloud{i}lennart" for i in range(num_nodes)]:
        kube_unmark_node(node, "role=driver:NoSchedule", "taint")
        kube_unmark_node(node, "driver-node", "label")

    driver_nodes = " ".join([f"cloud{i}lennart" for i in range(num_drivers)])
    subprocess.run(f"kubectl taint nodes {driver_nodes} role=driver:NoSchedule", shell=True, check=True)
    subprocess.run(f"kubectl label nodes {driver_nodes} driver-node=true", shell=True, check=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Configure Kubernetes nodes for drivers and workers.')
    parser.add_argument('num_drivers', type=int, help='Number of driver nodes')
    args = parser.parse_args()

    with open(CONFIG, 'r') as file:
        config = yaml.safe_load(file)

    kube_configure_nodes(config['kubernetes']['nodes'], args.num_drivers)
