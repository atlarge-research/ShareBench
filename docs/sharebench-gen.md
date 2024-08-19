# ShareBench-Gen

_ShareBench-Gen_ is a workload generator for OLAP workloads based on the TPC-DS data set and queries.
It is intended to be used with _ShareBench-Base_ to perform real-world performance analysis studies, especially of distributed resource-sharing mechanisms and policies.

## Installation

It is recommended to follow the installation of [ShareBench-Base](/docs/sharebench-base.md#installation).
For ShareBench-Gen in isolation, only the step of setting up a Python environment is needed.

## Collecting Query Data

This repository includes statistics for a limited number of queries and date ranges, that can be used _out of the box_ for generating new workloads. If the statistics should be gathered anew (possibly because of a different system performance), or new queries should be added, the process described below can be used.

1. Follow the guide to [generate data](/docs/sharebench-base.md#generate-data).
2. If new queries or date ranges have been added, [re-build the image](/docs/sharebench-base.md#image-modifications).
3. Use the [`collect_query_stats.py`](/scripts/collect_query_stats.py) scripts to collect runtime statistics of the queries.
4. Collect and save the query data using the [`query-stats`](/notebooks/query-stats.ipynb) notebook.
5. Import the new data into the generator.

The queries can be found in the [`docker/queries`](/docker/queries/) directory, any file with a `.sql` extension in this folder will be considered a query. 
The [`docker/queries/dates.json`](/docker/queries/dates.json) file defines the date ranges to be used.

## Generating Workloads

The workload generator exists in the form of a [Jupyter notebook](/notebooks/workload-gen.ipynb). 
The notebook includes use case examples which should serve as a manual.

Note that after generating and saving a workload, the Docker image has to be re-built and pushed, which can be done using the [`image`](/scripts/image.py) script (as described in [_Image Modifications_](/docs/sharebench-base.md#image-modifications))

