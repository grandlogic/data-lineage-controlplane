# Data Lake Lineage ControlPlane  (DLC)

A control plane for your data to guide it along its way as it takes its journey through your data 
fabric. Provide API for managing data lineage and ETL orchestration in a data lake or other data ETL processing
ecosystems.

## Getting Started

Overview of DLO can found:
http://grandlogic.blogspot.com

### Prerequisites
The DLC framework is based on Python.

Python
```
python 3.6+
python packages: TBD
```

For managing your python environment and dependency packages it is recommended to 
install and use Anaconda/Conda for development:

https://anaconda.org/

### Installing

This project is organized as a python package named:
```
datalake-lineage
```

You can install the module for testing in your local env from this git repo:
```
> cd my_repos
> git clone ssh://git@stash.grandlogic.com:5100/data/datalake-orchestration.git
> cd datalake-orchestration/src/python
> python setup.py install # same as `pip install .`
```
This will install the `datalake-lineage` package to your local python env.


Normal pip installation:
```
pip install datalake-lineage
```

Simple code example to verify you can access this packages's modules/functions:
```
> python
import da.dlake as dlo
grandlogic.dlo.joke()
```

## Running unit tests


Run all unit tests:
```
> cd datalake-orchestration/src/python
> python -m unittest
```

Run specific unit tests:
```
> cd data-pipeline/src/python
> python -m unittest test.grandlogic.dlo.test_simple_test
```

## DLC -  Data Lake Lineage Management API
This is a simple example for using the Python API for coordinating ETL processes 
in a data lake or other data movement pipeline. 
This example shows how ETL processes can use the DLO framework to manage data sets being 
made available for consumption by other downstream processes.

```
import os
import mysql.connector as mysql_connector

cls = TestZoneDataFlowCoordinator
dataflow_coord = ZoneDataFlowCoordinator(db_host=cls.db_host, db_user=cls.db_user, db_password=cls.db_password,
                                         db_name=cls.db_name)
 
# Create dataflow processes/models
dataflow_proc_rz = dataflow_coord.create_dataflow("rz", "billing4", "acme", "key_sec1")
print("RZ dataflow id: %s" % dataflow_proc_rz)
dataflow_proc_sz = dataflow_coord.create_dataflow("sz", "consol_billing5", "key1", "key_sec1")
print("SZ dataflow id: %s" % dataflow_proc_sz)
 
# Associate the rz dflow to the sz dflow
# This is saying that sz dflow depends and data from rz dflow and should be
# consume data produced by the rz dflow
dataflow_coord.associate_source_to_target(dataflow_proc_rz, dataflow_proc_sz)
 
# Start the the rz process
start_run_info_rz = dataflow_coord.start_dataflow_run_with_id(dataflow_proc_rz)
#
# Do actual ETL stuff in raw zone here......
#
# When ETL is all done, mark the dataflow process as finished for this run id
dataflow_coord.finish_dataflow_run('success',
                                   start_run_info_rz.run_id,
                                   record_count=89898,
                                   dataflow_batch_run_id='batchID123456',
                                   dataflow_partition_key="import_date=2019-01-01")
 
#
# Now imagine this is next set of code is another app/thread (airflow, step functions, lambda....etc
# that waiting to perform a Standard Zone ETL dataflow and is looking
# for data to be ready from the Raw Zone. In this example, this would be the process
# defined above 'dataflow_proc_sz', and already associated to the RZ dataflow above.
#
# Check for when new sources are ready for 'dataflow_proc_sz' to process.
# This can be a polling type of check that is performed periodically for 'dataflow_proc_sz'
all_ready_dataflow_source_runs = dataflow_coord.fetch_ready_dataflow_source_by_target_id(dataflow_proc_sz)
 
# Show RZ source(s) we found that this SZ dataflow is waiting for.
for ready_source_run in all_ready_dataflow_source_runs:
    print("Found a RZ source dataflow ready to be consumed by this SZ dataflow")
    print("target dataflow id: %s" % ready_source_run.target_dataflow_id)
    print("source zone: {}".format(ready_source_run.zone))
    print("source dataflow id: %s" % ready_source_run.source_dataflow_id)
    print("batch data id: {}".format(ready_source_run.dataflow_batch_run_id))
    print("partition key: {}".format(ready_source_run.dataflow_partition_key))
    print("rowcount: {}".format(ready_source_run.record_count))
    print("model name: %s" % ready_source_run.model_name)
    print("model key: %s" % ready_source_run.model_key)
    print("model sec key: %s" % ready_source_run.model_sec_key)
 
# Since we detected above sources that are ready for this SZ to process, we will now START the dataflow
# in the SZ.
start_result_sz = dataflow_coord.start_dataflow_run_with_id(dataflow_proc_sz)
 
# Info about the sources this SZ dataflow can consume
print("SZ run_id created {}".format(start_result_sz.run_id))
print("Number of RZ sources found {}".format(len(start_result_sz.dataflow_queue_list)))
for ready_source_run in start_result_sz.dataflow_queue_list:
    print("Found a RZ source dataflow ready to be consumed by this SZ dataflow")
    print("target dataflow id: %s" % ready_source_run.target_dataflow_id)
    print("source zone: {}".format(ready_source_run.zone))
    print("source dataflow id: %s" % ready_source_run.source_dataflow_id)
    print("batch data id: {}".format(ready_source_run.dataflow_batch_run_id))
    print("partition key: {}".format(ready_source_run.dataflow_partition_key))
    print("rowcount: {}".format(ready_source_run.record_count))
    print("model name: %s" % ready_source_run.model_name)
    print("model key: %s" % ready_source_run.model_key)
    print("model sec key: %s" % ready_source_run.model_sec_key)
 
#
# Do SZ ETL stuff... The ETL can use the above RZ source run metadata in its ETL process
# such as record_count, dataflow_batch_run_id, and dataflow_partition_key.
#
# When ETL is all done, mark the dataflow process as finished for this run id
dataflow_coord.finish_dataflow_run('success', start_result_sz.run_id, record_count=103,
                                   dataflow_batch_run_id='batchID89898989', dataflow_partition_key="some_partition_key")
 
#
# We are done, since this SZ dataflow has no downstream targets!
#
```
## Deployment

How to zip and deploy this project as a python package
