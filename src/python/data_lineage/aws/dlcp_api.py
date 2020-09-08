import os
import json
import boto3
import logging
from datetime import date, datetime, timedelta
import time
import jsonpickle
from typing import List, Set
from data_lineage.dataset_lineage_mgmt import DatasetLineage
from data_lineage.dataset_structs import DatasetFetchSummary, DatasetQueue, DatasetRun, DatasetStartResult
from data_lineage.dataset_exceptions import DatasetBaseException

logger = logging.getLogger()
logger.setLevel(logging.INFO)
client_lambda = boto3.client('lambda')

def get_dataflow_id(zone:str, model_name:str, model_key:str, model_sec_key:str = None) -> str:
    """
    Find unique GUID for dataflow.
    :param dataflow_id:
    :return: None if match not found.
    """

    payload = {"orchAction": "get_dataflow_id",
               "zone": zone,
               "model_name": model_name,
               "model_key": model_key,
               "model_sec_key": model_sec_key}

    json_payload = json.dumps(payload)
    pickledJsonObj = __boto_call(json_payload)

    obj: str = jsonpickle.decode(pickledJsonObj)
    return obj


def clear_dataflow_run(dataflow_run_id:str):
    """
    Clear an orphaned dataflow run.
    :param dataflow_run_id:
    :return:
    """

    payload = {"orchAction": "clear_dataflow_run",
               "dataflow_run_id": dataflow_run_id}

    json_payload = json.dumps(payload)
    pickledJsonObj = __boto_call(json_payload)

    obj: str = jsonpickle.decode(pickledJsonObj)
    return obj


def start_dataflow_run_with_id(
        dataflow_process_id: str,
        dependency_check:str = "any",
        specific_sources: List[str] = None,
        ext_job_run_key: str = None,
        ext_job_run_output_log_link: str = None,
        ext_etl_proc_key: str = None,
        ext_etl_proc_output_log_link: str = None) -> PdlDataflowStartResult:
    """
    Mark that a dataflow process has started running.

    Only one process can be actively running per dataflow at any one time.

    :param dataflow_process_id: (Required) Dataflow process ID
    :param dependency_check: {'all', 'any', 'ignore', 'source_ids', 'source_run_ids'}.
                             Run dataflow only when source dataflow dependency is met. Note that if target has no
                             source dependencies/associations the dataflow is always run.
                             'all': When all the associated source dependencies are ready.
                             'any': When at least one of the associated source dependencies is found ready.
                             'ignore': Start this dataflow run irrespective of any source dependencies.
                             'source_ids': List of source ids. See specific_sources param.
                             'source_run_ids': List of source run ids. See specific_sources param.
    :param specific_sources: A list of source ID or source run ids depending on value in dependency_check. Only the
                             provided ids are matched (must be subset of associated sources or source run ids).
                              If no matches found throws DataflowDependencyException.

                             Throws DataflowDependencyException if can't start run given this
                             dependency check rule.
    :param ext_job_run_key:
    :param ext_job_run_output_log_link:
    :param ext_etl_proc_key:
    :param ext_etl_proc_output_log_link:
    :raise: DataflowAlreadyActiveException if dataflow_process_id is already active.
    :raise: DataflowNotFoundException if dataflow_process_id not found.
    :raise: DataflowDependencyException if dependencies missing or not satisfied.
    :return: PdlDataflowStartResult containing run_id and any available list of sources run metadata
    (PdlDataflowQueue).
    """

    payload = {"orchAction": "start_by_id",
               "dataflow_process_id": dataflow_process_id,
               "dependency_check": dependency_check,
               "specific_sources": specific_sources,
               "ext_job_run_key": ext_job_run_key ,
               "ext_job_run_output_log_link": ext_job_run_output_log_link,
               "ext_etl_proc_key": ext_etl_proc_key,
               "ext_etl_proc_output_log_link": ext_etl_proc_output_log_link}

    json_payload = json.dumps(payload)
    pickledJsonObj = __boto_call(json_payload)

    obj: DataflowStartResult = jsonpickle.decode(pickledJsonObj)
    return obj


def start_dataflow_run_with_keys(
        zone:str, model_name:str, model_key:str, model_sec_key:str=None,
        dependency_check:str = "any",
        specific_sources: List[str] = None,
        ext_job_run_key: str = None,
        ext_job_run_output_log_link: str = None,
        ext_etl_proc_key: str = None,
        ext_etl_proc_output_log_link: str = None) -> PdlDataflowStartResult:
    """
    Mark that a dataflow process has started running.

    Only one process can be actively running per dataflow at any one time.

    :param zone: {'rz', 'sz', 'cz', 'xz'}
    :param model_name: e.g. for sz: "billing" for cz: "consolidated_billing"
    :param model_key: e.g. "health_net=Piedmont"
    :param model_sec_key: e.g. "prov_code=SGHSHF"
    :param dependency_check: {'all', 'any', 'ignore', 'source_ids', 'source_run_ids'}.
                             Run dataflow only when source dataflow dependency is met. Note that if target has no
                             source dependencies/associations the dataflow is always run.
                             'all': When all the associated source dependencies are ready.
                             'any': When at least one of the associated source dependencies is found ready.
                             'ignore': Start this dataflow run irrespective of any source dependencies.
                             'source_ids': List of source ids. See specific_sources param.
                             'source_run_ids': List of source run ids. See specific_sources param.
    :param specific_sources: A list of source ID or source run ids depending on value in dependency_check. Only the
                             provided ids are matched (must be subset of associated sources or source run ids).
                              If no matches found throws DataflowDependencyException.

                             Throws DataflowDependencyException if can't start run given this
                             dependency check rule.
    :param ext_job_run_key:
    :param ext_job_run_output_log_link:
    :param ext_etl_proc_key:
    :param ext_etl_proc_output_log_link:
    :raise: DataflowAlreadyActiveException if dataflow_process_id is already active.
    :raise: DataflowNotFoundException if dataflow_process_id not found.
    :raise: DataflowDependencyException if dependencies missing or not satisfied.
    :return: PdlDataflowStartResult containing run_id and any available list of sources run metadata
    (PdlDataflowQueue).
    """

    payload = {"orchAction": "start_by_keys",
               "zone": zone,
               "model_name": model_name,
               "model_key": model_key,
               "model_sec_key": model_sec_key,
               "dependency_check": dependency_check,
               "specific_sources": specific_sources,
               "ext_job_run_key": ext_job_run_key ,
               "ext_job_run_output_log_link": ext_job_run_output_log_link,
               "ext_etl_proc_key": ext_etl_proc_key,
               "ext_etl_proc_output_log_link": ext_etl_proc_output_log_link}

    json_payload = json.dumps(payload)
    pickledJsonObj = __boto_call(json_payload)

    obj: PdlDataflowStartResult = jsonpickle.decode(pickledJsonObj)
    return obj


def finish_dataflow_run(status: str,
                        dataflow_run_id: str,
                        record_count: int = None,
                        dataflow_batch_run_id: str = None,
                        dataflow_partition_key: str = None,
                        ext_job_run_key: str = None,
                        ext_job_run_output_log_link: str = None,
                        ext_etl_proc_key: str = None,
                        ext_etl_proc_output_log_link: str = None):
    """
            Close a currently started dataflow run id currently.

            :param status: {'error', 'success'}
            :param dataflow_run_id: Unique GUID acquired when dataflow was started.
            :param record_count: Optional record count
            :param dataflow_batch_run_id: Optional 32 char alpha numeric
            :param dataflow_partition_key: Optional text field. Can be json metadata for source keys/partitions.
            :param ext_job_run_key: Optional key to external job processing metadata
            :param ext_job_run_output_log_link: Optional link to external job processing metadata
            :param ext_etl_proc_key: Optional key to ETL  processing metadata
            :param ext_etl_proc_output_log_link: Optional link to ETL job processing metadata
            :raise: DataflowRunNotFoundException if run_id not found or dataflow not running
            :raise: DataflowRunAlreadyFinishedException
            :return:
            """

    payload = {"orchAction": "finish",
               "status": status,
               "dataflow_run_id": dataflow_run_id,
               "record_count": record_count,
               "dataflow_batch_run_id": dataflow_batch_run_id,
               "dataflow_partition_key": dataflow_partition_key,
               "ext_job_run_key": ext_job_run_key,
               "ext_job_run_output_log_link": ext_job_run_output_log_link,
               "ext_etl_proc_key": ext_etl_proc_key,
               "ext_etl_proc_output_log_link": ext_etl_proc_output_log_link}

    json_payload = json.dumps(payload)
    pickledJsonObj = __boto_call(json_payload)

    msg = jsonpickle.decode(pickledJsonObj)
    return msg


def fetch_ready_dataflow_source_by_target_id(target_dataflow_id: str,
                                             dependency_check: str = 'any') -> (List[PdlDataflowQueue],
                                                                                PdlDataflowFetchSummary):
    """
    Detect if any sources are ready for consumption for a target.

    :param target_dataflow_id:
    :param dependency_check: {'all', 'any'} Fetch dataflow sources ready to run only when 'any' or 'all' of the
                             dependency sources are present. Default is 'any'. If 'all" and number of unique sources
                             ready are less than the number of dependencies a target has then empty list is returned.
    :return: Empty list if no matching sources found and/or dependency_check not satisfied.
    """

    # now invoke this lambda again with the event/counter
    payload = {"orchAction": "fetch_by_id",
               "target_dataflow_id": target_dataflow_id,
               "dependency_check": dependency_check}

    json_payload = json.dumps(payload)
    pickledJsonObj = __boto_call(json_payload)

    obj: FetchResult = jsonpickle.decode(pickledJsonObj)
    return obj.queueInfo, obj.summary


def fetch_ready_dataflow_source_by_target_keys(zone: str = None,
                                               model_name: str = None,
                                               model_key: str = None,
                                               model_sec_key: str = None,
                                               dependency_check: str = 'any')-> (List[PdlDataflowQueue],
                                                                                 PdlDataflowFetchSummary):
    """
    Detect if any sources are ready for consumption for a target.

    :param zone: Zone may be provided alone or with additional predicates below to narrow dataflows of interest.
    :param model_name Optional. Zone is required.
    :param model_key Optional. model_name is required.
    :param model_sec_key Optional. model_key is required.
    :param dependency_check: {'all', 'any'} Fetch dataflow sources ready to run only when 'any' or 'all' of the
                             dependency sources are present. Default is 'any'. If 'all" and number of unique sources
                             ready are less than the number of dependencies a target has then empty list is returned.
    :return: Empty list if no matching sources found and/or dependency_check not satisfied.
    """

    # now invoke this lambda again with the event/counter
    payload = {"orchAction": "fetch_by_keys",
               "zone": zone,
               "model_name": model_name,
               "model_key": model_key,
               "model_sec_key": model_sec_key,
               "dependency_check": dependency_check}

    json_payload = json.dumps(payload)
    pickledJsonObj = __boto_call(json_payload)

    obj: FetchResult = jsonpickle.decode(pickledJsonObj)
    return obj.queueInfo, obj.summary


def __boto_call(json_payload):
    """

    :param json_payload:
    :return: pickled object represented in JSON form
    """

    response = client_lambda.invoke(
        FunctionName='pdl-orch-service-lambda',
        InvocationType='RequestResponse',
        Payload=json_payload)

    logger.info(response)
    dataResult = json.loads(response['Payload'].read().decode())
    logger.info("dataResult: %s:" % dataResult)

    service_status_code = dataResult.get('serviceStatusCode')
    if service_status_code is None:
        raise DataFlowBaseException("Internal/Unknown error.")
    print("serviceStatusCode: %i: " % service_status_code)

    if service_status_code != 200:
        raise DataFlowBaseException(dataResult["body"])

    return json.loads(dataResult["body"])


if __name__ == '__main__':
    logger.addHandler(logging.StreamHandler())
    #queueInfo, summary = fetch_ready_dataflow_source_by_target_id(target_dataflow_id="98770cff448741b49bb3d9a30f2c23de")
    dataflow_id = get_dataflow_id("cz", "consolidated_billing", "na", "na")
    logger.info("body un-pickled")
    logger.info("dataflow_id: %s" % dataflow_id)
    #logger.info("sources in queue: %s" % len(queueInfo))
    exit()