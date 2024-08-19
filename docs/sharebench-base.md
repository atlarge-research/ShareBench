# ShareBench-Base

_ShareBench-Base_ is an infrastructure framework for automated real-world performance analysis studies of distributed resource-sharing mechanisms and policies.

## Prerequisites

To use the infrastructure framework and properly utilize its features, multiple prerequisites need to be satisfied.

### Software

_ShareBench-Base_ is developed for Linux operating systems. The framework has been tested on Ubuntu 20.04.3, mileage with other versions may differ.

Furthermore needed is Python in at least version 3.12 (tested with version 3.12.4).

### Kubernetes

The infrastructure framework requires access to a Kubernetes cluster via `kubectl`. This cluster may be emulated on the same (base) machine also hosting the framework or located remotely and distributed over multiple/many machines. 

For the Spark application to run, the cluster should have a service account with the name `spark` and a cluster role binding withe `edit` rights for this account. The account and role binding can be created with the following command:

```bash
kubectl create serviceaccount spark && \
kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=default:spark --namespace=default
```

## Installation

Most of the installation of the framework is automated in a script, with only a few manual steps needed.

### Python

It is recommended to use a [virtual Python environment](https://docs.python.org/3/library/venv.html). Such an environment can be created and activated as follows:

```bash
python3.12 -m venv .venv && source .venv/bin/activate
```

The needed packages can then be installed with pip:

```bash
pip install -r requirements.txt
```

### Config

The [`config.yaml.template`](/config.yaml.template) file provides a template configuration for the framework. To use the file, rename (or copy) the file to remove the `.template` extension. Most parameters can be left unchanged, however, some need to be configured based on the environment. These include:

- `kubernetes/<ip, port>` - IP address and port of the Kubernetes cluster.
- `kubernetes/ssh_keyfile` - SSH keyfile for access to the Kubernetes nodes (needed for the Telegraf installation).
- `kubernetes/<nodes, memory, cpu>` - Available resource of the Kubernetes nodes.
- `docker/username` - Username of a valid Docker account that shall be used for the Spark image.
- `services/general/ip` - (Local) IP address of the host machine.

### Hosts

The Telegraf installation must know the addresses of all Kubernetes nodes. These should be specified in a `hosts.txt` file as a list of `user@host` entries. The exact format is also shown in the [`hosts.txt.template`](/hosts.txt.template) file.

### Framework

The framework can be installed by running the [`install`](/scripts/install.py) script:

```bash
python3 scripts/install.py
```

To only install a specific component, the `-t` flag can be used to specify a target installation step.

## Configuration

The configuration files of most components are dynamically generated to include parameters from the config. The [`config.yaml`](/config.yaml) file is therefore the first step to modify the configurations. 

If the values that should be modified are not present as parameters in the file, the configuration files can be modified directly. For this, it is important to modify the template files, found in the [`templates`](/templates/) folder - Any changes to the _applied_ files will be overwritten once the configurations are re-applied! As a rule of thumb: If the file starts with _"WARNING: Edits in this file will be overwritten..."_ it should not be edited directly.

For any changes to take effect, the configuration files need to be re-applied:

```bash
python3 scripts/apply_configurations.py
```

Also here, a (list of) specific target configuration(s) may be specified with the `-t` flag.

## Image Modifications
For any modifications to the Spark Docker image (e.g., adding new workloads, including additional files, modifying the Scala code) the appropriate files need to be edited/added. The image can then be rebuilt and pushed using a simple command:

```bash
python3 scripts/image.py -b
```

If the program code has not been modified, the `-b` flag can be omitted to avoid re-compiling the program.

## Usage

This section will describe some example use cases of the framework. It is to note that these these use cases are not exhaustive. The full capabilities of the framework are best understood by _manually_ exploring its features.

### Generate Data

Before any queries or workloads can be run, the TPC-DS data set needs to be generated.
For this, the [`generate_data`](/scripts/generate_data.py) script can be used:

```bash
python3 scripts/generate_data.py
```

Optionally, only the data generation or only the metadata creation can be selected by specifying the `-m` flag with the options `data` and `meta` respectively.

The scale of the data set can currently only be set directly by changing the `DB_SCALE_FACTOR` constant in the [`ShareBench.scala`](/src/main/scala/ShareBench.scala) file.

Depending on the data set scale and the available resources the data generation can take up to multiple hours. Creation of the metadata is typically much faster.

### Run Workload

After [generating a workload](sharebench-gen.md#generating-workloads), such a workload can be run using the [`run_workload`](/scripts/run_workload.py) script:

``` bash
python3 scripts/run_workload.py [-m <mechanism>] <num_apps> <workload> <start_delay>
```

All data (metrics) of the workload run will, upon successful completion automatically be copied into the [`data`](/data) folder. The output of the script will include a line specifying the `SessionID`, which is comprised of the workload name and the start time. This ID is needed to identify the results.

### Analyze Results

To analyze the results of a workload run, the framework includes various [Jupyter notebooks](/notebooks/). All notebook include examples of use that should be sufficient in explaining their use.

### Run Experiment

ShareBench includes a feature to automate multiple workload runs with various configurations.
Experiments can be defined as so-called _recipe books_ in the form of YAML files.
A single recipe book can contain one to many _recipes_ which in turn can define one to many workload executions.

The recipe book format is best explained in an [example](/experiments/recipe-books/example.yaml).
Each recipe needs to specify a list of workloads, mechanisms, and number of apps.
For recipes that do not specify some property, the default values (defined at the top of the file) will be used.

Recipe books can be executed with the [`run_experiment`](/scripts/run_experiment.py) script:

```bash
python3 scripts/run_experiment.py <recipe path>
```

The `--redirect_stdout` and `--redirect_stderr` flags can be used to enable redirection of standard out and standard error respectively to a file (saved in the [`experiments/output`](/experiments/output/) directory).
Additionally, the start delay can be customized with the `--start-delay` flag (defaults to 45 seconds).

Executing a recipe book will sequentially execute all recipes (in order as specified) and for each recipe sequentially execute all combinations of values (in order as specified).
The loop structure is `workload` -> `mechanism` -> `num_apps`, from outermost to innermost.

Results with the SessionIDs (needed for visualization/analysis) of each run will be saved in the [`experiments/results`](/experiments/results/) folder under the name of the recipe book and identified by the start time of the recipe.

## Useful Tools

Here are some commands and tools that may be handy when working with ShareBench.

### Nohup

Run a command as background process (useful for running longer experiments):

```bash
nohup <command> &
```

The started process will not be terminated even if the shell session exits. All output will be written to a `nohup.out` file. 

### Kubectl

Get logs of most recent Spark Driver:

```bash
kubectl logs $(kubectl get pods -A --sort-by=.metadata.creationTimestamp | grep driver | tail -n 1 | awk '{print $2}') -n $(kubectl get pods -A --sort-by=.metadata.creationTimestamp | grep driver | tail -n 1 | awk '{print $1}')
```

Get logs of most recent Spark Driver but filter for all lines that don't start with a timestamp:
```bash
kubectl logs $(kubectl get pods -A --sort-by=.metadata.creationTimestamp | grep driver | tail -n 1 | awk '{print $2}') -n $(kubectl get pods -A --sort-by=.metadata.creationTimestamp | grep driver | tail -n 1 | awk '{print $1}') --follow | grep -v "^[0-9]\{2\}/[0-9]\{2\}/[0-9]\{2\} [0-9]\{2\}:[0-9]\{2\}:[0-9]\{2\}"
```

Append `--follow` to either of the commands keep the log open.
