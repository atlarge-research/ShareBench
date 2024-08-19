# ShareBench

_ShareBench_ is a tool set for real-world performance analysis studies of distributed resource-sharing mechanisms and policies, developed as part of my Computer Science Bachelor Thesis on _performance characterization of distributed resource-sharing mechanisms_.

The work consists of two components: [_ShareBench-Base_](docs/sharebench-base.md) and [_ShareBench-Gen_](docs/sharebench-gen.md). The former is an infrastructure framework for automated real-world performance analysis studies. The latter is a workload generator for OLAP workloads based on the TPC-DS data set and queries.
Documentation for each of the components can be found in the [`docs`](docs/) folder.

The only supported composition is Spark on Kubernetes. Support for other Application Frameworks or Resource Managers is not currently planned.

The work was initially based on the [spark-data-generator](https://github.com/sacheendra/spark-data-generator/) by Sacheendra Talluri. Code annotations indicate what material is copied/adapted from that source.

## Experiment Data
To reduce the size of this repository, the data obtained through the experiments is not directly included but rather provided in [sharebench-data](https://github.com/lkm-schulz/sharebench-data).

## Citation

When using ShareBench for research, please use the following BibTeX entry for citations:
```bibtex
@thesis{schulz2024Sharebench,
    author = {Schulz, Lennart K. M.},
    title = {{ShareBench}: Performance Characterization of Distributed Resource-Sharing Mechanisms},
    institution = {{VU} Amsterdam},
    type = {Bachelor Thesis},
    date = {2024},
}
```
