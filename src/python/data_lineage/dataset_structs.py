from datetime import datetime
from typing import List, Set


class DatasetRun:
    def __init__(self, dataset_id:str,
                 dataset_run_id:str,
                 status:int,
                 start_dt:datetime,
                 end_dt:datetime=None,
                 rowcount:int=None,
                 dataset_batch_run_id:str=None,
                 dataset_partition_key:str=None,
                 ext_job_run_key:str=None,
                 ext_job_run_output_log_link:str=None,
                 ext_etl_proc_key:str=None,
                 ext_etl_proc_output_log_link:str=None):

        self.dataset_id=dataset_id
        self.dataset_run_id=dataset_run_id
        self.status=status
        self.start_dt=start_dt
        self.end_dt=end_dt
        self.rowcount=rowcount
        self.dataset_batch_run_id=dataset_batch_run_id
        self.dataset_partition_key=dataset_partition_key
        self.ext_job_run_key=ext_job_run_key
        self.ext_job_run_output_log_link=ext_job_run_output_log_link
        self.ext_etl_proc_key=ext_etl_proc_key
        self.ext_etl_proc_output_log_link=ext_etl_proc_output_log_link


class DatasetProperties:
    def __init__(self, dataset_process_id:str, zone:int, model_name:str, model_namespace:str,
                 model_partition_keys:str = None, status:bool = None, description:str = None,
                 created_dt:datetime = None, status_update_dt:datetime = None):
        self.zone=zone
        self.dataset_process_id=dataset_process_id
        self.model_name=model_name
        self.model_namespace=model_namespace
        self.model_partition_keys=model_partition_keys
        self.description=description
        self.status = status
        self.created_dt = created_dt
        self.status_update_dt = status_update_dt


class DatasetQueue:
    def __init__(self,
                 sink_dataset_id: str,
                 source_dataset_id: str,
                 source_run_id: str,
                 run_config: str,
                 run_breadcrumb: str,
                 dataset_batch_run_id: str,
                 dataset_run_metadata: str,
                 dataset_record_count: int,
                 source_ready_dt: datetime,
                 sink_zone: int,
                 sink_model_name: str,
                 sink_model_namespace: str,
                 sink_model_dataset_props: str,
                 source_zone: int,
                 source_model_name: str,
                 source_model_namespace: str,
                 source_model_dataset_props: str,
                 ):
        self.sink_dataset_id=sink_dataset_id
        self.source_dataset_id=source_dataset_id
        # These next ones are dynamic metadata about the source
        self.source_run_id=source_run_id
        self.run_config=run_config
        self.run_breadcrumb=run_breadcrumb
        self.dataset_batch_run_id=dataset_batch_run_id
        self.dataset_run_metadata=dataset_run_metadata
        self.dataset_record_count=dataset_record_count
        self.source_ready_dt=source_ready_dt
        # These next ones are static metadata about the source
        self.sink_zone=sink_zone
        self.sink_model_name=sink_model_name
        self.sink_model_namespace=sink_model_namespace
        self.sink_model_dataset_props=sink_model_dataset_props
        self.source_zone = source_zone
        self.source_model_name = source_model_name
        self.source_model_namespace = source_model_namespace
        self.source_model_dataset_props = source_model_dataset_props

class DatasetStartResult:
    def __init__(self, run_id, sink_id: str, dataset_queue_list: List[DatasetQueue], source_sink_rel_count: int,
                 source_id_list: List[str], source_run_id_list: List[str],
                 orphan_sink: bool):
        """

        :param run_id: Run id of sink dataset
        :param sink_id: sink_id start results apply to.
        :param dataset_queue_list: List of available sources and their metadata and run data.
        :param source_sink_rel_count: Count of statically defined source/sink relationships
        :param source_id_list: Actual set of sources that are ready/available to consume from
        :param orphan_sink: Is sink currently in possible orphan/run state
        """
        self.run_id = run_id
        self.sink_id = sink_id
        self.dataset_queue_list = dataset_queue_list
        self.source_sink_rel_count = source_sink_rel_count
        self.source_id_list = source_id_list  # list of unique sources that are ready to be consumed by this sink
        self.source_run_id_list = source_run_id_list
        self.orphan_sink = orphan_sink


class DatasetFetchSummary:
    def __init__(self, dataset_queue_list: List[DatasetQueue], source_sink_rel_count: int,
                 source_id_list: List[str], sink_id: str, source_run_id_list: List[str],
                 orphan_sink: bool):
        """

        :param dataset_queue_list: List of available sources and their metadata and run data.
        :param source_sink_rel_count: Count of statically defined source/sink relationships
        :param source_id_list: Actual set of sources that are ready/available to consume from
        :param sink_id_list: List of sink_id_list. Can be more than one when searching across a zone/keys.
        :param orphan_sink: Is sink currently in possible orphan/run state
        """

        self.dataset_queue_list = dataset_queue_list
        self.source_sink_rel_count = source_sink_rel_count
        self.source_id_list = source_id_list  # list of unique sources that are ready to be consumed by this sink
        self.sink_id = sink_id
        self.source_run_idList = source_run_id_list
        self.orphan_sink = orphan_sink

