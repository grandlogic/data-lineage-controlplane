import os
import sys
import json
import logging
# from datetime import date, datetime, timedelta
# import time
from data_lineage.dataset_lineage_mgmt import DatasetLineage
from data_lineage.dataset_structs import DatasetFetchSummary, DatasetQueue, DatasetRun, DatasetStartResult
from data_lineage.dataset_exceptions import DatasetBaseException

import mysql.connector as mysql_connector
from mysql.connector import Error
import jsonpickle

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:

    db_host = os.environ['PDL_DB_HOST']
    db_user = os.environ['PDL_DB_USER']
    db_password = os.environ['PDL_DB_PASS']
    db_name = 'pdl'

    db_con = mysql_connector.connect(
        host=db_host,
        user=db_user,
        passwd=db_password,
        database=db_name
    )

except Error as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit()

orch_mgr = ZoneDataFlowCoordinator(db_con)
logger.info("SUCCESS: Orchestration Manager is set up!")

def lambda_handler(event, context):
    result = do_work(event, context)

    return result


def do_work(event=None, context=None):

    # Let's figure out the type of request this is. The options are:
    # start dataflow for a target
    # finish dataflow for a target
    # check available sources for a given dataflow target if they are ready

    if event is None:
        return error_result("No event found")

    if event.get('orchAction') is None:
        return error_result("No event orchestration action found")

    event_map = {
        "start_by_id": start_by_id,
        "start_by_keys": start_by_keys,
        "finish": finish,
        "fetch_by_id": fetch_by_id,
        "fetch_by_keys": fetch_by_keys,
        "get_dataflow_id": get_dataflow_id,
        "clear_dataflow_run": clear_dataflow_run
    }
    action_func = event_map.get(event.get('orchAction'), error_result("Unknown action: %s" % event.get('orchAction')))

    return action_func(event)


def error_result(error_msg):
    return {
        'serviceStatusCode': 400,
        'body': json.dumps(error_msg)
    }


def success_result(resultObj):

    jsonPickled = jsonpickle.encode(resultObj)
    logger.info("jsonPickled: %s" % jsonPickled)

    return {
        'serviceStatusCode': 200,
        'body': json.dumps(jsonPickled)
    }


def get_dataflow_id(event):

    try:
        result = orch_mgr.get_dataflow_id(zone=event.get('zone'),
                                          model_name=event.get('model_name'),
                                          model_key=event.get('model_key'),
                                          model_sec_key=event.get('model_sec_key'))

        return success_result(result)
    except Exception as ex:
        return error_result("Exception: %s" % ex)


def clear_dataflow_run(event):

    try:
        result = orch_mgr.clear_dataflow_run(dataflow_run_id=event.get('dataflow_run_id'))

        return success_result(result)
    except Exception as ex:
        return error_result("Exception: %s" % ex)


def start_by_id(event):

    try:
        result = orch_mgr.start_dataflow_run_with_id(dataflow_process_id=event.get('dataflow_process_id'),
                                                     dependency_check=event.get('dependency_check'),
                                                     specific_sources=event.get('specific_sources'),
                                                     ext_job_run_key=event.get('ext_job_run_key'),
                                                     ext_job_run_output_log_link=event.get('ext_job_run_output_log_link'),
                                                     ext_etl_proc_key=event.get('ext_etl_proc_key'),
                                                     ext_etl_proc_output_log_link=event.get('ext_etl_proc_output_log_link'))

        return success_result(result)
    except Exception as ex:
        return error_result("Exception: %s" % ex)


def start_by_keys(event):

    try:
        result = orch_mgr.start_dataflow_run_with_keys(zone=event.get('zone'),
                                                   model_name=event.get('model_name'),
                                                   model_key=event.get('model_key'),
                                                   model_sec_key=event.get('model_sec_key'),
                                                   dependency_check=event.get('dependency_check'),
                                                   specific_sources=event.get('specific_sources'),
                                                   ext_job_run_key=event.get('ext_job_run_key'),
                                                   ext_job_run_output_log_link=event.get('ext_job_run_output_log_link'),
                                                   ext_etl_proc_key=event.get('ext_etl_proc_key'),
                                                   ext_etl_proc_output_log_link=event.get('ext_etl_proc_output_log_link'))

        return success_result(result)
    except Exception as ex:
        return error_result("Exception: %s" % ex)


def finish(event):

    try:
        orch_mgr.finish_dataflow_run(status=event.get('status'),
                                     dataflow_run_id=event.get('dataflow_run_id'),
                                     record_count=event.get('record_count'),
                                     dataflow_batch_run_id=event.get('dataflow_batch_run_id'),
                                     dataflow_partition_key=event.get('dataflow_partition_key'),
                                     ext_job_run_key=event.get('ext_job_run_key'),
                                     ext_job_run_output_log_link=event.get('ext_job_run_output_log_link'),
                                     ext_etl_proc_key=event.get('ext_etl_proc_key'),
                                     ext_etl_proc_output_log_link=event.get('ext_etl_proc_output_log_link'))

        return success_result("dataflow is finished!")
    except Exception as ex:
        return error_result("Exception: %s" % ex)


def fetch_by_id(event):

    try:

        result1, result2 = orch_mgr.fetch_ready_dataflow_source_by_target_id(
            target_dataflow_id=event.get('target_dataflow_id'),
            dependency_check=event.get('dependency_check'))

        return success_result(FetchResult(result1, result2))
    except Exception as ex:
        return error_result("DataFlow Exception: %s" % ex)


def fetch_by_keys(event):

    try:
        result1, result2 = orch_mgr.fetch_ready_dataflow_source_by_target_keys(
            model_name=event.get('model_name'),
            model_key=event.get('model_key'),
            model_sec_key=event.get('model_sec_key'),
            dependency_check=event.get('dependency_check'))

        return success_result(FetchResult(result1, result2))
    except Exception as ex:
        return error_result("Exception: %s" % ex)


if __name__ == '__main__':
    logger.addHandler(logging.StreamHandler())
    do_work()