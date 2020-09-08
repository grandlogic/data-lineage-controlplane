import mysql.connector as dbapi_connector
# from mysql.connector import Error as dbapi_error
from  mysql.connector.pooling import MySQLConnectionPool as dbapi_pool
# from datetime import datetime
from src.python.data_lineage.uuid_util import get_new_guid
import uuid
import os
# from typing import List, Set
from . db_layer import DBManager
from . dataset_exceptions import *
from . dataset_structs import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dataset-app")
logger.setLevel(logging.INFO)


### A dataset lineage tracing, dataset dependency modeling, dataset inlet/outlet tracking, and overall ETL process monitoring.
### Build realtime and historical reports and visualize of ETL processing status (success, failures, orphaned, retries, replays).
### Current and historical dataset dependences and ETL processing dataset source/sink relationships.


def a_simple_func():
    return "I am talking to you"

class DatasetLineage:

    def __init__(self,
                 db_con:dbapi_connector = None,
                 db_pool:dbapi_pool = None,
                 db_host:str = None, db_user:str = None, db_password:str = None, db_name:str = None):
        """
        Can pass in a db_con and manage the connection from the outside, pass a db pool and let the class
        request and return (close) connections from the pool, or pass db params to
        this class and let it create/destroy db connections.

        Pool is useful to use if this is long lived object and/or will be passed around and used

        :param db_con: If provided, will let caller manage db con pooling and closing.
        :param db_host:
        :param db_user:
        :param db_password:
        :param db_name:
        """

        self.dbmgr = DBManager(db_con, db_pool, db_host, db_user, db_password, db_name)

        if db_con:
            self.db_con:dbapi_connector = db_con
            self.db_type=1
        elif db_pool:
            self.db_pool:dbapi_pool = db_pool
            self.db_type = 2
        else:
            self.dbconfig = {
                "host": db_host,
                "user" : db_user,
                "password": db_password,
                "database": db_name
            }

            self.db_con = dbapi_connector.connect(**self.dbconfig)

        # self.dbapi_pooling = dbapi_pooling.MySQLConnectionPool(pool_name="o_pool", pool_size=3, **self.dbconfig)

        self.db_con.autocommit = True

    def close_db_con(self):

        self.dbmgr.close_db_con()

        if self.db_type == 1: # external con
            return
        elif self.db_type == 2: # external pool
            return
        else:  # let user explicitly close internally managed con
            if self.db_con and self.db_con.is_connected:
                self.db_con.close()
                self.db_con = None
            else:
                self.db_con = None

    def clear_dataset_observer_run(self, dataset_run_id:str):
        """
        Clear an orphaned dataset run.

        ToDo: clear_dataset_run() Might need another api call also to clear any/all for an overall dataset_observer
              and not just a for a specific run?? This should also clear the queue table so sources are shown
              as not consumed yet.
        :return:
        """
        pass

    def update_dataset_observer(self, dataset_observer_id:str, description:str=None,
                                observer_config:str=None,
                                display_name:str=None) ->int:

        """

        :param dataset_observer_id:
        :type dataset_observer_id:
        :param description:
        :type description:
        :param observer_status:
        :type observer_status: 0 disabled, 1 enabled, 2 retired. disabled means can't run (even if sinks ready),
        retired can't be used as new source for a sink dataset/observer
        :param observer_config:
        :type observer_config:
        :return: Records updated. Should be 1. If 0, means dataset_id does not exist.
        :rtype:
        """
        conn = None
        try:
            # dataset_process_id = get_new_guid()
            conn = self.__get_db_con()
            cursor = conn.cursor()

            col_name_list = []
            col_val_list = []
            if display_name is not None:
                col_name_list.append("display_name")
                col_val_list.append(display_name)
            if description is not None:
                col_name_list.append("description")
                col_val_list.append(description)
            if observer_config is not None:
                col_name_list.append("observer_config")
                col_val_list.append(observer_config)

            if len(col_name_list) == 0:
                raise APIValidationException("Validation error: Missing update fields to update!")

            stmt_update_sql = "UPDATE dataset_observer SET "

            for i in range(len(col_name_list)):
                if i > 0:
                    stmt_update_sql += ', '
                stmt_update_sql += col_name_list[i] + ' = %s'

            stmt_update_sql += ", status_update_dt = %s WHERE dataset_observer_id = %s"
            logger.debug("SQL stmt: %s" % stmt_update_sql)
            input_vals = (col_val_list)
            input_vals.append(datetime.now())
            input_vals.append(dataset_observer_id)
            cursor.execute(stmt_update_sql, input_vals)
            rows_updated = cursor.rowcount
            # conn.commit()
            cursor.close()
            return rows_updated
        except Exception as err:
            logger.error("Internal operation failure: {}".format(err))
            raise InternalDatasetException from err
        finally:
            if conn is not None:
                self.__close_db_con(conn)

    def update_dataset_observer_status(self, dataset_observer_id:str,
                                       observer_status:int=None) ->int:

        """

        :param dataset_observer_id:
        :type observer_status: possible values: 0 disabled, 1 enabled, 2 retired.
        disabled means can't run (even if sources ready), retired can't be used as new source
        for a sink dataset/observer when making associations.
        :return: Records updated. Should be 1. If 0, means dataset_id does not exist.
        :rtype:
        """
        conn = None
        try:
            #dataset_process_id = get_new_guid()
            conn = self.__get_db_con()

            # if trying to retire and dataset_id has source/sink rels then throw validation exception
            # that tells you can't
            if observer_status == 2:
                validate_cursor = conn.cursor()
                val_sql = "SELECT count(*) FROM dataset_source_to_sink_meta_rel WHERE " \
                          "source_dataset_id = %s or sink_dataset_id = %s and terminated_dt = %s"
                input_vals = (observer_status, observer_status, self.dbmgr.get_max_datetime_to_sec())
                validate_cursor.execute(val_sql, input_vals)

                rows = validate_cursor.fetchall()
                validate_cursor.close()

                if len(rows) > 0:
                    # Found a current record
                    count_of_recs = rows[0][0]
                    if count_of_recs > 0:
                        raise DatasetNotFoundException("Cannot retire: There are multiple sources/sinks associated with this observer")

            cursor = conn.cursor()
            col_name_list = []
            col_val_list = []
            if observer_status is not None:
                col_name_list.append("observer_status")
                col_val_list.append(observer_status)

            if len(col_name_list) == 0:
                raise APIValidationException("Validation error: Missing fields to update!")

            if observer_status is not None and (isinstance(observer_status, bool) or observer_status not in (0, 1, 2)):
                raise APIValidationException("Validation error: Observer status not valid. Must be 0, 1, 2: %s" % observer_status)

            stmt_update_sql = "UPDATE dataset_observer SET "

            for i in range(len(col_name_list)):
                if i > 0:
                    stmt_update_sql += ', '
                stmt_update_sql += col_name_list[i] + ' = %s'

            stmt_update_sql += ", status_update_dt = %s WHERE dataset_observer_id = %s"
            logger.debug("SQL stmt: %s" % stmt_update_sql)
            input_vals = (col_val_list)
            input_vals.append(datetime.now())
            input_vals.append(dataset_observer_id)
            cursor.execute(stmt_update_sql, input_vals)
            rows_updated = cursor.rowcount
            # conn.commit()
            cursor.close()
            return rows_updated
        except Exception as err:
            logger.error("Internal operation failure: {}".format(err))
            raise InternalDatasetException from err
        finally:
            if conn is not None:
                self.__close_db_con(conn)

    def delete_dataset_observer(self, dataset_observer_id:str):
        """
        A dataset observer ID can only be deleted if it is disabled and has no current associations or past runs.
        If has past runs, deleted them first, then disable, then delete dataset observer.
        :param dataset_observer_id:
        :return:
        """
        pass

    def declare_dataset_observer(self,
                                 model_name:str,
                                 model_namespace:str = 'ROOT',
                                 model_dataset_props: str = 'NA',
                                 model_zone_tag: int = 1,
                                 observer_config:str=None,
                                 display_name:str=None,
                                 description:str=None) -> str:
        """
        Declare a new dataset that will track the ETL for a data model within
        a particular data lake zone.

        There can be only one process per zone/model_name/namespace/model-keys.

        Once successfully declared and enabled (enabled by default), a dataset agent can consume events
        from the corresponding ETL process.

        :param model_name: Model name for data model managed by this dataset process. Usually humanly understandable
        name.
        :param model_namespace: Optional namespace for model. If you want to group the model names into namespaces.
        :param model_dataset_props: Partition keys. Can be JSON or other name=value type of string structure.
        :param model_zone_tag: You can define your own data lake zones starting with 0 and go up. Defaults to 0.
        :param description: Give some description if you like.
        :param config_data: Static configuration information the observer can pass to the ETL process.
        :return: guid hex string for newly tracked dataset process
        """

        conn = None
        try:
            dataset_observer_id = get_new_guid()
            conn = self.__get_db_con()
            cursor = conn.cursor()

            if model_zone_tag < 0:
                raise DatasetBaseException('Invalid zone_tag value. The zone tag is: {}'.format(model_zone_tag))

            # ToDo: See if it exists already, if it does, then enable it (if disabled) and return the same existing one.

            stmt_insert = "INSERT INTO dataset_observer (model_name, model_namespace, model_dataset_props, model_zone_tag, " \
                          "dataset_observer_id, description, observer_config, display_name) " \
                          "VALUES ( %s, %s, %s, %s, %s, %s, %s, %s)"
            input_vals = (model_name, model_namespace, model_dataset_props, model_zone_tag,
                          dataset_observer_id.hex, description, observer_config, display_name)
            cursor.execute(stmt_insert, input_vals)
            cursor.close()

            return dataset_observer_id.hex
        except Exception as err:
            logger.error("Internal operation failure: {}".format(err))
            raise InternalDatasetException from err
        finally:
            if conn is not None:
                self.__close_db_con(conn)

    def start_dataset_observer_run_with_id(self,
                                           dataset_observer_id: str,
                                           dependency_check="any",
                                           specific_sources: List[str] = None,
                                           breadcrumb: str = None,
                                           ext_job_run_key: str = None,
                                           ext_job_run_output_log_link: str = None,
                                           ext_etl_proc_key: str = None,
                                           ext_etl_proc_output_log_link: str = None) -> DatasetStartResult:
        """
        Mark that a dataset process has started running.

        Only one process can be actively running per dataset at any one time.

        :param dataset_observer_id: (Required) dataset process ID
        :param dependency_check: {'all', 'any', 'ignore', 'source_ids', 'source_run_ids'} or List of source dataset IDs.
                                 Run dataset only when source dataset dependency is met. Note that if sink has no
                                 source dependencies/associations the dataset is always run.
                                 'all': When all the associated source dependencies are ready (at least one of each).
                                 'any': When at least one of the associated source dependencies is found ready.
                                 'ignore': Start this dataset run irrespective of any source dependencies.
                                 'source_ids': When list of source ids (subset of source/sink rels) found to be ready.
                                 See specific_sources param.
                                 'source_run_ids': List of source run ids. See specific_sources param.
        :param specific_sources: A list of source ID or source run ids depending on value in dependency_check. Only the
                                 provided ids are matched (must be subset of associated sources or source run ids).
                                 If no matches found throws DependencyException.

                                 Throws DependencyException if can't start run given this
                                 dependency check rule.
        :param breadcrumb: Customizable field at start of run that contain runtime breadcrumb trail info/metadata or metadata
        for retry scenarios.
        :param ext_job_run_key:
        :param ext_job_run_output_log_link:
        :param ext_etl_proc_key:
        :param ext_etl_proc_output_log_link:
        :raise: datasetAlreadyActiveException if dataset_observer_id is already active.
        :raise: datasetNotFoundException if dataset_observer_id not found.
        :raise: datasetDependencyException if dependencies missing or not satisfied.
        :return: PdldatasetStartResult containing run_id and any available list of sources run metadata
        (PdldatasetQueue).
        """

        conn = None
        try:
            conn = self.__get_db_con()

            if self.__does_dataset_observer_id_exist(conn, dataset_observer_id) == False:
                raise DatasetNotFoundException('Invalid dataset process id. The value was: {}'.format(dataset_observer_id))

            return self.__internal_start_dataset_run(conn, dataset_observer_id,
                                                     dependency_check=dependency_check,
                                                     specific_sources=specific_sources,
                                                     breadcrumb=breadcrumb,
                                                     ext_job_run_key=ext_job_run_key,
                                                     ext_job_run_output_log_link=ext_job_run_output_log_link,
                                                     ext_etl_proc_key=ext_etl_proc_key,
                                                     ext_etl_proc_output_log_link=ext_etl_proc_output_log_link)
        finally:
            if conn is not None:
                self.__close_db_con(conn)

    def start_dataset_observer_run_with_keys(self,
                                             model_name: str,
                                             model_namespace: str = 'ROOT',
                                             model_dataset_props: str = 'NA',
                                             model_zone_tag: int = 1,
                                             dependency_check='any',
                                             specific_sources: List[str] = None,
                                             breadcrumb: str = None,
                                             ext_job_run_key:str = None,
                                             ext_job_run_output_log_link:str = None,
                                             ext_etl_proc_key:str = None,
                                             ext_etl_proc_output_log_link:str = None) -> DatasetStartResult:
        """
        Mark that a dataset process has started running.

        Only one process can be actively running per dataset at any one time.

        :param model_zone_tag: {'rz', 'sz', 'cz', 'xz'}
        :param model_name: e.g.
        :param model_namespace:
        :param model_dataset_props: e.g.
        :param dependency_check: {'all', 'any', 'ignore', 'source_ids', 'source_run_ids'} or List of source dataset IDs.
                                 Run dataset only when source dataset dependency is met. Note that if sink has no
                                 source dependencies/associations the dataset is always run.
                                 'all': When all the associated source dependencies are ready.
                                 'any': When at least one of the associated source dependencies is found ready.
                                 'ignore': Start this dataset run irrespective of any source dependencies.
                                 'source_ids': List of source ids. See specific_sources param.
                                 'source_run_ids': List of source run ids. See specific_sources param.
        :param specific_sources: A list of source ID or source run ids depending on value in dependency_check. Only the
                                 provided ids are matched (must be subset of associated sources or source run ids).
                                  If no matches found throws datasetDependencyException.

                                 Throws DatasetDependencyException if can't start run given this
                                 dependency check rule.
        :param breadcrumb: Customizable field at start of run that contain runtime breadcrumb trail info/metadata or metadata
        for retry scenarios.
        :param ext_job_run_key:
        :param ext_job_run_output_log_link:
        :param ext_etl_proc_key:
        :param ext_etl_proc_output_log_link:
        :raise: AlreadyActiveException if dataset_observer_id is already active or orphaned.
        :raise: DatasetNotFoundException if dataset_observer_id not found.
        :raise: DependencyException if dependencies missing or not satisfied.
        :return: DatasetStartResult containing run_id and any available list of sources run metadata (DatasetQueue).
        """

        conn = None

        try:
            conn = self.__get_db_con()

            if model_dataset_props is None:
                model_dataset_props = model_namespace

            dataset_observer_id = self.__lookup_dataset_id(conn,
                                                           model_name,
                                                           model_namespace,
                                                           model_dataset_props,
                                                           model_zone_tag)

            if dataset_observer_id is None:
                raise DatasetNotFoundException("dataset process does not exist, so sorry can't run")

            return self.__internal_start_dataset_run(conn, dataset_observer_id,
                                                     dependency_check=dependency_check,
                                                     specific_sources=specific_sources,
                                                     breadcrumb=breadcrumb,
                                                     ext_job_run_key=ext_job_run_key,
                                                     ext_job_run_output_log_link=ext_job_run_output_log_link,
                                                     ext_etl_proc_key=ext_etl_proc_key,
                                                     ext_etl_proc_output_log_link=ext_etl_proc_output_log_link)
        finally:
            if conn is not None:
                self.__close_db_con(conn)

    def finish_dataset_observer_run(self, status: str,
                                    dataset_run_id: str,
                                    record_count: int = None,
                                    dataset_batch_run_id: str = None,
                                    dataset_partition_key: str = None,
                                    ext_job_run_key: str = None,
                                    ext_job_run_output_log_link: str = None,
                                    ext_etl_proc_key: str = None,
                                    ext_etl_proc_output_log_link: str = None
                                    ):
        """
        Close a currently started dataset run id currently.

        :param status: {'error', 'success'}
        :param dataset_run_id: Unique GUID acquired when dataset was started.
        :param record_count: Optional record count
        :param dataset_batch_run_id: Optional 32 char alpha numeric
        :param dataset_partition_key: Optional text field. Can be json metadata for source keys/partitions.
        :param ext_job_run_key: Optional key to external job processing metadata
        :param ext_job_run_output_log_link: Optional link to external job processing metadata
        :param ext_etl_proc_key: Optional key to ETL  processing metadata
        :param ext_etl_proc_output_log_link: Optional link to ETL job processing metadata
        :raise: datasetRunNotFoundException if run_id not found or dataset not running
        :raise: datasetRunAlreadyFinishedException
        :return:
        """

        # Query for run record. Error if no run record is updated/found or not in proper state (ready or started)
        # UPDATE run record if all goes well
        # Do not queue up any downstream datasets if status error, just commit and return

        # Query for dataset details using run_id, if find source/sink rels, then queue up records
        # INSERT queue record (more than one possible) based on source/sink rels found
        # Above two steps are done via INSERT/SELECT

        conn = None

        try:
            status_map = {
                "error": 0,
                "success": 3
            }
            status_val = status_map.get(status, 4)  # default to rz

            if status_val == 4:
                raise DatasetBaseException('Invalid status type. The status value was: {}'.format(status))

            conn = self.__get_db_con()

            dlc_dataset_run = self.__lookup_dataset_run_id(conn, dataset_run_id)

            if dlc_dataset_run is None:
                raise DatasetNotFoundException('dataset run ID not found'.format(dataset_run_id))

            if dlc_dataset_run.status in [0, 3, 4]:
                raise RunAlreadyFinishedException('dataset run ID not active/running. Current status: '.format(dlc_dataset_run.status))

            current_dt = datetime.now()

            try:
                conn.start_transaction()

                update_run_cursor = conn.cursor()

                col_name_list = []
                col_type_list = []
                col_val_list = []

                if dataset_batch_run_id is not None:
                    col_name_list.append('dataset_batch_run_id')
                    col_type_list.append("%s")
                    col_val_list.append(dataset_batch_run_id)

                if dataset_partition_key is not None:
                    col_name_list.append('dataset_partition_key')
                    col_type_list.append("%s")
                    col_val_list.append(dataset_partition_key)

                if record_count is not None:
                    col_name_list.append('record_count')
                    col_type_list.append("%s")
                    col_val_list.append(record_count)

                col_name_list.append('end_dt')
                col_type_list.append("%s")
                col_val_list.append(current_dt)

                col_name_list.append('status')
                col_type_list.append("%s")
                col_val_list.append(status_val)

                if ext_job_run_key is not None:
                    col_name_list.append('ext_job_run_key')
                    col_type_list.append("%s")
                    col_val_list.append(ext_job_run_key)

                if ext_job_run_output_log_link is not None:
                    col_name_list.append('ext_job_run_output_log_link')
                    col_type_list.append("%s")
                    col_val_list.append(ext_job_run_output_log_link)

                if ext_etl_proc_key is not None:
                    col_name_list.append('ext_etl_proc_key')
                    col_type_list.append("%s")
                    col_val_list.append(ext_etl_proc_key)

                if ext_etl_proc_output_log_link is not None:
                    col_name_list.append('ext_etl_proc_output_log_link')
                    col_type_list.append("%s")
                    col_val_list.append(ext_etl_proc_output_log_link)

                # col_name_list.append('run_id')
                # col_type_list.append("%s")
                col_val_list.append(dataset_run_id)

                #col_list_str = ", ".join(col_name_list)
                #col_type_list_str = ", ".join(col_type_list)

                stmt_update_run = 'UPDATE dataset_observer_run SET '
                for i in range(len(col_name_list)):
                    if i > 0:
                        stmt_update_run += ', '
                    stmt_update_run +=  col_name_list[i] + ' = %s'

                stmt_update_run += ' WHERE run_id = %s AND (status = 1 or status = 2)'
                #print("stmt update run {}".format(stmt_update_run))
                input_vals = col_val_list
                update_run_cursor.execute(stmt_update_run, input_vals)

                #print("update_run_cursor rowcount: {}".format(update_run_cursor.rowcount))

                if update_run_cursor.rowcount != 1:
                    update_run_cursor.close()
                    conn.rollback()
                    raise RunNotFoundException("Could not find datasource run to update status. Either bad run id or run id already finalized {}".format(dataset_run_id))

                update_run_cursor.close()

                ########

                ## Do not queue up any downstream datasets if error status. We are done here!!
                if status == 'error':
                    conn.commit()
                    return
                #####

                # Insert/Select to find sink dataset id and all dataset sources
                # May create multiple queue records

                stmt_insert_q = """INSERT INTO 
                                       dataset_source_sink_event_queue (
                                       dataset_rel_id, -- sink_dataset_id, source_dataset_id,
                                       source_run_id, source_ready_dt)
                                   SELECT
                                       dataset_rel_id, %s, %s
                                       FROM dataset_source_to_sink_meta_rel rel
                                          -- JOIN dataset_observer source ON (source.dataset_observer_id = )
                                       WHERE source_dataset_id = %s AND terminated_dt = %s"""

                insert_q_cursor = conn.cursor()

                #print("stmt insert/select into q {}".format(stmt_insert_q))
                input_vals = (dataset_run_id, current_dt, dlc_dataset_run.dataset_id, self.dbmgr.get_max_datetime_to_sec())
                insert_q_cursor.execute(stmt_insert_q, input_vals)
                insert_q_cursor.close()

                conn.commit()
            finally:
                if conn.in_transaction:
                    conn.rollback()
        finally:
            if conn is not None:
                self.__close_db_con(conn)

    def get_dataset_observer_run(self, dataset_run_id:str) -> DatasetRun:
        """
        ToDo get_dataset_run()
        :param dataset_run_id:
        :return:
        """
        pass

    def get_dataset_observer(self, dataset_observer_id: str) -> DatasetProperties:
        """
        Find a matching dataset.
        :param dataset_observer_id:
        :return: None if no match
        """
        conn = None

        try:
            conn = self.__get_db_con()
            cursor = conn.cursor()

            stmt_query = "SELECT dataset_process_id, model_zone_tag, model_name, model_namespace, " \
                         "model_dataset_props, status, description, created_dt, status_update_dt " \
                         "FROM dataset_observer WHERE dataset_observer_id = %s "
            input_vals = (dataset_observer_id,)
            cursor.execute(stmt_query, input_vals)

            resultset = cursor.fetchall()

            dlcp_model = None
            for row in resultset:
                dlcp_model = DatasetProperties(
                    dataset_process_id=row[0],
                    zone=row[1],
                    model_name=row[2],
                    model_namespace=row[3],
                    model_partition_keys=row[4],
                    status=row[5],
                    description=row[6],
                    created_dt=row[7],
                    status_update_dt=row[8]
                )

            cursor.close()
            return dlcp_model
        finally:
            if conn is not None:
                self.__close_db_con(conn)

    def get_dataset_observer_id(self,
                                model_name: str,
                                model_namespace: str = 'ROOT',
                                model_dataset_props: str = 'NA',
                                model_zone_tag: int = 1,
                                ) -> str:
        """
        Find unique GUID for dataset.

        :param dataset_process_id:
        :param zone:
        :param model_name:
        :param model_namespace:
        :param model_partition_keys:
        :return:
        """
        conn = None

        try:
            conn = self.__get_db_con()

            return self.__lookup_dataset_id(conn,
                                            model_name,
                                            model_namespace,
                                            model_dataset_props,
                                            model_zone_tag)
        finally:
            if conn is not None:
                self.__close_db_con(conn)

    def associate_dataset_source_to_sink(self, source_dataset_id:str, sink_dataset_id:str):
        """
        Define a relationship between sink dataset and source dataset.

        :param sink_dataset_id:
        :param source_dataset_id:
        :return: ID of association. If already associated, then id just returned.
        """
        # ToDo: Validation logic to insure that zones association are always go left to right (e.g. low to high)
        #       or within same zone and that the dataset_ids actually exist.
        #
        # 1) Make sure both datasets exist.
        # 2) Check if has any existing active associations, if so short-circuit and return current rel id.
        # 3) If no current active association, insert new record and return rel id.
        #

        conn = None
        try:

            conn = self.__get_db_con()
            # conn.start_transaction()

            # both source and sink must exist and be enabled, in order to do an association
            validate_cursor = conn.cursor()
            sql_select = "SELECT count(*) FROM dataset_observer WHERE " \
                         "dataset_observer_id IN (%s, %s) and observer_status = 1"

            select_input_vals = (source_dataset_id, sink_dataset_id)
            validate_cursor.execute(sql_select, select_input_vals)
            rows = validate_cursor.fetchall()
            validate_cursor.close()

            if len(rows) > 0:
                # Found a current record
                count_of_recs = rows[0][0]
                if count_of_recs == 1:
                    raise DatasetNotFoundException("One of source or sink observers not found or are retired")
                elif count_of_recs == 0:
                    raise DatasetNotFoundException("Both source and sink observers not found or are retired")

            select_cursor = conn.cursor()
            sql_select = "SELECT dataset_rel_id FROM dataset_source_to_sink_meta_rel WHERE " \
                          "source_dataset_id = %s AND sink_dataset_id = %s and terminated_dt = %s"

            select_input_vals = (source_dataset_id, sink_dataset_id, self.dbmgr.get_max_datetime_to_sec())
            select_cursor.execute(sql_select, select_input_vals)
            rows = select_cursor.fetchall()
            select_cursor.close()

            if len(rows) > 0:
                # Found a current record
                current_dataset_rel_id = rows[0][0]
                return current_dataset_rel_id

            rel_id = get_new_guid().hex
            cursor = conn.cursor()
            stmt_insert = 'INSERT INTO dataset_source_to_sink_meta_rel (source_dataset_id, sink_dataset_id, dataset_rel_id) ' \
                          'VALUES (%s, %s, %s)'
            input_vals = (source_dataset_id, sink_dataset_id, rel_id)
            cursor.execute(stmt_insert, input_vals)
            # conn.commit()
            cursor.close()
            return rel_id
        except Exception as err:
            logger.error("Internal operation failure: {}".format(err))
            raise InternalDatasetException from err
        finally:
            if conn is not None:
                self.__close_db_con(conn)

    def disassociate_dataset_source_from_sink(self, source_dataset_id:str, sink_dataset_id:str) ->bool:
        """
        :param source_id:
        :param sink_id:
        :return: True if disassociated, False if never was associated to begin with.
        """
        conn = None
        try:

            conn = self.__get_db_con()
            # conn.start_transaction()dataset_source_to_sink_meta_rel

            cursor = conn.cursor()
            stmt_update = 'UPDATE dataset_source_to_sink_meta_rel SET terminated_dt = current_timestamp WHERE ' \
                          'source_dataset_id = %s and sink_dataset_id = %s and terminated_dt = %s'
            input_vals = (source_dataset_id, sink_dataset_id, self.dbmgr.get_max_datetime_to_sec())
            cursor.execute(stmt_update, input_vals)
            num_rows_updated = cursor.rowcount
            # conn.commit()
            cursor.close()
            if num_rows_updated == 0:
                return False
            else:
                return True
        except Exception as err:
            logger.error("Internal operation failure: {}".format(err))
            raise InternalDatasetException from err
        finally:
            if conn is not None:
                self.__close_db_con(conn)

    def disassociate_all_dataset_sources_from_sink(self, sink_id:str):
        """
        :param sink_id:
        :return:
        """

        conn = None
        try:
            conn = self.__get_db_con()
            # conn.start_transaction()
            cursor = conn.cursor()
            stmt_delete = 'DELETE FROM dataset_source_to_sink_rel WHERE ' \
                          'sink_dataset_id = %s'
            input_vals = (sink_id,)
            cursor.execute(stmt_delete, input_vals)
            # conn.commit()
            cursor.close()
        finally:
            if conn is not None:
                self.__close_db_con(conn)

    def disassociate_all_dataset_sinks_from_source(self, source_id:str):
        """
        :param source_id:
        :return:
        """

        conn = None
        try:
            conn = self.__get_db_con()
            # conn.start_transaction()
            cursor = conn.cursor()
            stmt_delete = 'DELETE FROM source_to_sink_rel WHERE ' \
                          'source_dataset_id = %s'
            input_vals = (source_id,)
            cursor.execute(stmt_delete, input_vals)
            # conn.commit()
            cursor.close()
        finally:
            if conn is not None:
                self.__close_db_con(conn)

    def fetch_ready_dataset_sources_by_sink_id(self,
                                                 sink_dataset_id: str
                                                 #dependency_check: str = 'any'
                                                 ) -> DatasetFetchSummary:
        """
        Detect if any sources are ready for consumption for a sink.

        :param sink_dataset_id:
        :param dependency_check: {'all', 'any'} Fetch dataset sources ready to run only when 'any' or 'all' of the
                                 dependency sources are present. Default is 'any'. If 'all" and number of unique sources
                                 ready are less than the number of dependencies a sink has then empty list is returned.
        :return: DatasetFetchSummary object returned
        """
        conn = None

        try:
            conn = self.__get_db_con()
            queue_list, unique_source_id_list, static_source_rel_count, sink_id, source_run_id_list, \
            orphan_sink, dataset_rel_id_list, sink_observer_config =\
                self.__internal_sources_ready_in_queue(conn, sink_dataset_id)

            fetch_result_summary = DatasetFetchSummary(queue_list, static_source_rel_count, unique_source_id_list,
                                                       sink_id, source_run_id_list, orphan_sink)

            print("queue_list: %s \n unique_source_id_list: %s \n static_source_rel_count: %d \n "
                  "unique_sink_id_list: %s \n orphan sink: %s" %
                  (len(queue_list), unique_source_id_list, static_source_rel_count, sink_id, orphan_sink))

            return fetch_result_summary
        finally:
            if conn is not None:
                self.__close_db_con(conn)

    def fetch_ready_dataset_sources_by_sink_keys(self,
                                                 model_name: str,
                                                 model_namespace: str = 'ROOT',
                                                 model_dataset_props: str = 'NA',
                                                 model_zone_tag: int = 1,
                                                 # dependency_check: str = 'any'
                                                 ) -> DatasetFetchSummary:
        """
        Detect if any sources are ready for consumption for a sink.

        :param model_zone_tag: Zone may be provided alone or with additional predicates below to narrow datasets of interest.
        :param model_name Optional
        :param model_dataset_props Optional
        :param model_namespace Optional
        :param dependency_check: {'all', 'any'} Fetch dataset sources ready to run only when 'any' or 'all' of the
                                 dependency sources are present. Default is 'any'. If 'all" and number of unique sources
                                 ready are less than the number of dependencies a sink has then empty list is returned.
        :return: DatasetFetchSummary object returned
        """

        conn = None
        try:
            conn = self.__get_db_con()

            dataset_queue_list, source_id_list, source_sink_rel_count, sink_id_list, source_run_id_list, \
            orphan_sink, sink_observer_config =\
                self.__internal_sources_ready_in_queue(conn,
                                                       model_name=model_name,
                                                       model_namespace=model_namespace,
                                                       model_zone_tag=model_zone_tag,
                                                       model_dataset_props=model_dataset_props
                                                       #dependency_check=dependency_check
                                                       )

            fetch_result_summary = DatasetFetchSummary(dataset_queue_list, source_sink_rel_count,
                                                       source_id_list, sink_id_list, source_run_id_list, orphan_sink)

            return fetch_result_summary
        finally:
            if conn is not None:
                self.__close_db_con(conn)

    def retry_dataset_observer(self, dataset_run_id):
        """
        Can retry datasets that have errored only. All sources will be recreated/duplicated. Orphaned
        datasets can't be retried until they have been explicitly cleared.
        :param dataset_run_id:
        :return:
        """
        pass

    def replay_dataset_observer(self, dataset_run_id):
        """
        Replay a datasets that was previously successful. All sources will be recreated/duplicated.
        :param dataset_run_id:
        :return:
        """
        pass

    def __get_db_con(self) -> dbapi_connector.connection:

        if self.db_type == 1: # an outside controlled con
            self.db_con.autocommit=True
            return self.db_con
        elif self.db_type == 2: # an outside con pool
            con = self.db_pool.get_connection()
            con.autocommit = True
            return con
        else: # db params and internally managed con
            if self.db_con is None:
                self.db_con = dbapi_connector.connect(**self.dbconfig)
                self.db_con.autocommit=True
                return self.db_con
            elif self.db_con.is_connected():
                self.db_con.autocommit = True
                return self.db_con
            else: # for some reason the DB con was disconnected, so create a new one
                self.db_con = dbapi_connector.connect(**self.dbconfig)
                self.db_con.autocommit = True
                return self.db_con

    def __close_db_con(self, conn:dbapi_connector):
        if self.db_type == 1:
            return
        elif self.db_type == 2:
            conn.close() # return to the pool
        else: # do not close internally managed con
            return

    def __lookup_dataset_id(self, conn,
                            model_name:str,
                            model_namespace:str='ROOT',
                            model_dataset_props:str='NA',
                            model_zone_tag:int = 1
                            ) -> str:
        """
                Find and return the dataset GUID , if one exists.

        :param conn:
        :param zone:
        :param model_name:
        :param model_namespace:
        :param model_partition_keys:
        :return:
        """
        cursor = conn.cursor()

        stmt_query = "SELECT dataset_observer_id FROM dataset_observer WHERE zone = %s " \
                     " AND model_name = %s AND model_namespace = %s AND model_dataset_props = %s AND model_zone_tag = %i"
        input_vals = (model_name, model_namespace, model_dataset_props, model_zone_tag)
        cursor.execute(stmt_query, input_vals)

        row = cursor.fetchall()
        if cursor.rowcount == 0:
            process_id = None
        else:
            process_id = row[0][0]

        cursor.close()

        return process_id

    def __lookup_dataset_run_id(self, conn:dbapi_connector.connection, run_id:str) -> DatasetRun:
        """

        :param conn:
        :param run_id:
        :return:
        """
        cursor = conn.cursor()

        stmt_query = "SELECT dataset_observer_id, run_id, status, start_dt  FROM dataset_observer_run WHERE run_id = %s "
        input_vals = (run_id,)
        cursor.execute(stmt_query, input_vals)

        result_set = cursor.fetchall()
        #if cursor.rowcount == 0:
        #    cursor.close()
        #    return None

        run_model = None
        for row in result_set:
            run_model = DatasetRun(
                dataset_id=row[0],
                dataset_run_id = row[1],
                status = row[2],
                start_dt = row[3]
                )

        cursor.close()

        return run_model

    def __does_dataset_observer_id_exist(self, conn, dataset_observer_id:str) -> bool:
        """
        Check if dataset process id exists.

        :param conn:
        :param dataset_observer_id:
        :return: False or True if process id exists
        """
        cursor = conn.cursor()

        #print("dataset_id {}".format(dataset_process_id))
        #print(get_dlc_raw_hex_to_byte(dataset_process_id))

        stmt_query = 'SELECT dataset_observer_id FROM dataset_observer WHERE dataset_observer_id = %s'
        input_vals = (dataset_observer_id,)
        cursor.execute(stmt_query, input_vals)

        row = cursor.fetchall()
        #print('Length {}'.format(len(row)))
        count = cursor.rowcount
        #print('Count: {}'.format(count))
        cursor.close()

        if count == 0:
            return False
        else:
            return True

    def __internal_sources_ready_in_queue(self, conn:dbapi_connector.connection,
                                          dataset_observer_id: str=None,
                                          model_name: str=None,
                                          model_namespace: str = 'ROOT',
                                          model_zone_tag: int = 0,
                                          model_dataset_props: str = 'NA',
                                          dependency_check='any',
                                          specific_sources: List[str] = None
                                          ) -> (List[DatasetQueue], Set[str], int, str,
                                                List[str], bool, Set[str], str):
        """
        Lookup all associated sources_dataset_run_ids runs and return rowcounts/dataset_batch_id
        and model keys for each source run IDs

        :param conn:
        :param dataset_observer_id: If present keys zone--->model_sec_key are ignored
        :param zone: Zone may be provided alone or with additional predicates below.
        :param model_name Optional. Zone is required.
        :param model_key Optional. model_name is required.
        :param model_sec_key Optional. model_key is required.
        :param dependency_check: {'all', 'any', 'source_ids', 'source_run_ids'} Fetch dataset sources ready to run
                                 only when 'any' or 'all' of the
                                 dependency sources are present. Default is 'any'. If 'all" and number of unique sources
                                 ready are less than the number of dependencies a sink has then empty list is
                                 returned. If list of source ids add to predicates to filter just on those source IDs.
                                 Can also be List of source dataset IDs.
        :param specific_sources: A list of source ID or source run ids depending on value in dependency_check. Only the
                                 provided ids are matched (must be subset of associated sources or source run ids).
        :return: return tuple of source queue/runs found, unique sources ids list found in queue,
                 static sink/source rel count, unique sink Ids list (can be more than one when searching using keys)
                 ,and list of source run ids.
        """

        # ToDo: Throw error if not list and is not 'all' or 'any' or 'ignore' or 'source_ids' or 'source_run_ids'
        # and if source ids specific_sources must have list > 0

        specific_source_id_list: List[str] = []
        # if type(dependency_check) is list:
        if specific_sources is not None:
            print("Filter on specific source_ids")
            specific_source_id_list = specific_sources

        col_name_list = []
        col_val_list = []

        # either use dataset_id or the dataset process keys used to define/create a dataset
        if dataset_observer_id is not None:
            col_name_list.append('z.dataset_observer_id')
            col_val_list.append(dataset_observer_id)
        elif model_zone_tag is not None and \
             model_name is not None and \
             model_namespace is not None and \
             model_dataset_props is not None:
            col_name_list.append('z.model_zone_tag')
            col_val_list.append(model_zone_tag)
            col_name_list.append('z.model_name')
            col_val_list.append(model_name)
            col_name_list.append('z.model_namespace')
            col_val_list.append(model_namespace)
            col_name_list.append('z.model_dataset_props')
            col_val_list.append(model_dataset_props)
        else:
            raise DatasetBaseException("dataset_id or keys are missing for lookup up dataset sources")

        # get count of actual sources associated with sink. Static meta relationships.
        count_cursor = conn.cursor()
        count_query = """
                     SELECT 
                          count(rel.source_dataset_id) num_sources, max(z.dataset_observer_id) the_sink_proc_id 
                     FROM dataset_observer z 
                         LEFT JOIN dataset_source_to_sink_meta_rel as rel
                             ON (z.dataset_observer_id = rel.sink_dataset_id AND rel.terminated_dt = %s) 
                     WHERE
                     """

        for index, col_name in enumerate(col_name_list):
            if index > 0:
                count_query += " AND "
            count_query += col_name + "=%s"
        # count_query += " GROUP BY z.dataset_observer_id"
        #print(count_query)
        input_vals = (self.dbmgr.get_max_datetime_to_sec(), *col_val_list)
        #print(input_vals)
        count_cursor.execute(count_query, input_vals)

        # a nice hack to get the sink_proc_id even if it is not explicitly passed in this function.
        the_sink_proc_id = None

        row = count_cursor.fetchone()
        static_source_rel_count=0
        # dont' really need a for loop here, since only on row max can be returned
        #for row in rows:
        if row is not None:
            static_source_rel_count = row[0]
            the_sink_proc_id = row[1]
        else:
            count_cursor.close()
            raise DatasetBaseException("dataset_id or keys were not found")

        count_cursor.close()

        # should never happen. defensive code
        if dataset_observer_id is not None and the_sink_proc_id != dataset_observer_id:
            raise DatasetBaseException("The provided dataset_id does match records in query. dataset_observer_id: %s, "
                                       "the_sink_proc_id: %s", dataset_observer_id, the_sink_proc_id)

        # if static_source_rel_count let next sql run, just in case to handle cases something in went wrong with rel
        # static vs rel runtime.

        # find all the sources ready for processing by sink
        sources_rel_cursor = None
        if static_source_rel_count > 0:
            sources_rel_cursor = conn.cursor()
        stmt_query = """
                     SELECT 
                         mrel.sink_dataset_id, mrel.source_dataset_id, qu.source_run_id,
                         run.run_observer_config, run.run_breadcrumb, 
                         run.batch_run_id, run.run_metadata, run.record_count, 
                         z.model_name, z.model_zone_tag, z.model_namespace, z.model_dataset_props,
                         run.status, qu.source_ready_dt, 
                         source.model_name, source.model_zone_tag, source.model_namespace, source.model_dataset_props, 
                         qu.dataset_rel_id, qu.rerun_status, qu.rerun_last_sink_run_id, z.observer_config
                     FROM dataset_source_sink_event_queue as qu
                     JOIN dataset_source_to_sink_meta_rel as mrel
                          ON (mrel.dataset_rel_id = qu.dataset_rel_id)
                     JOIN dataset_observer_run as run 
                          ON (qu.source_run_id = run.run_id) 
                     JOIN dataset_observer as z -- sink
                          ON (z.dataset_observer_id = mrel.sink_dataset_id) 
                     JOIN dataset_observer as source 
                          ON (source.dataset_observer_id = mrel.source_dataset_id) 
                     WHERE qu.sink_run_id = 'zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz' AND run.status = 3  
                     """
        #for col_name in col_name_list:
        #    stmt_query += " AND " + col_name + "=%s"
        stmt_query += " AND z.dataset_observer_id = %s"

        len_source = len(specific_source_id_list)
        for idx, source_id in enumerate(specific_source_id_list):
            if idx == 0:
                if dependency_check == 'source_ids':
                    stmt_query += " AND mrel.source_dataset_id IN ("
                else:
                    stmt_query += " AND qu.source_run_id IN ("

            if idx == (len_source - 1):
                stmt_query += "%s)"
            else:
                stmt_query += "%s,"

        stmt_query += " ORDER BY qu.source_ready_dt ASC"

        # print(stmt_query)

        #total_col_val_list = col_val_list + specific_source_id_list
        # print(total_col_val_list)

        #input_vals = (total_col_val_list, )
        input_vals = (the_sink_proc_id, *specific_source_id_list)
        # print("input vals")
        # print(input_vals)

        rows = []
        if sources_rel_cursor is not None:
            sources_rel_cursor.execute(stmt_query, input_vals)
            rows = sources_rel_cursor.fetchall()

        queue_list = []
        result_source_id_list = []
        #sink_id_list = []
        dataset_run_id_list = []
        dataset_rel_id_list = []
        sink_observer_config=""
        for row in rows:
            queue = DatasetQueue(
                sink_dataset_id=row[0],
                source_dataset_id=row[1],
                source_run_id=row[2],
                run_config=row[3],
                run_breadcrumb=row[4],
                dataset_batch_run_id=row[5],
                dataset_run_metadata=row[6],
                dataset_record_count=row[7],

                sink_model_name=row[8],
                sink_zone=row[9],
                sink_model_namespace=row[10],
                sink_model_dataset_props=row[11],

                source_ready_dt=row[13],

                source_model_name=row[14],
                source_zone=row[15],
                source_model_namespace=row[16],
                source_model_dataset_props=row[17]
            )

            result_source_id_list.append(queue.source_dataset_id)
            #sink_id_list.append(queue.sink_dataset_id)
            dataset_run_id_list.append(queue.source_run_id)
            queue_list.append(queue)
            dataset_rel_id_list.append(row[18])
            sink_observer_config = row[19]

        if sources_rel_cursor is not None:
            sources_rel_cursor.close()

        # Check and report if sink has orphans
        orphan_query = "SELECT run_id, status FROM dataset_observer_run" \
                      " WHERE dataset_observer_id = %s AND (status = 1 OR status = 2)"
        input_vals = (the_sink_proc_id,)
        orphan_cursor = conn.cursor()
        orphan_cursor.execute(orphan_query, input_vals)

        orphan_sink = False
        rows = orphan_cursor.fetchall()
        if orphan_cursor.rowcount > 0:
            orphan_sink = True
        orphan_cursor.close()


        if static_source_rel_count != len(set(result_source_id_list)) and dependency_check == 'all':
            # return empty list
            return [], set(result_source_id_list), static_source_rel_count, the_sink_proc_id, dataset_run_id_list, \
                   orphan_sink, set(dataset_rel_id_list), sink_observer_config

        # source queue/runs found, unique sources ids found in queue, static sink/source rel count
        return queue_list, set(result_source_id_list), static_source_rel_count, the_sink_proc_id, \
            dataset_run_id_list, orphan_sink, set(dataset_rel_id_list), sink_observer_config

    def __internal_start_dataset_run(self, conn, dataset_observer_id:str,
                                     dependency_check='any',
                                     specific_sources: List[str] = None,
                                     breadcrumb: str = None,
                                     ext_job_run_key: str = None,
                                     ext_job_run_output_log_link: str = None,
                                     ext_etl_proc_key: str = None,
                                     ext_etl_proc_output_log_link: str = None) -> DatasetStartResult:
        """

        :param conn:
        :param dataset_observer_id:
        :param dependency_check: {'all', 'any', 'ignore', 'source_ids', 'source_run_ids'} or List of source dataset IDs.
                                 Run dataset only when source dataset dependency is met. If sink has no
                                 source dependencies/associations the dataset is always run.
                                 'all': When all the associated source dependencies are ready.
                                 'any': When at least one of the associated source dependencies is found ready.
                                 'ignore': Start this dataset run irrespective of any source dependencies.
                                 'source_ids': List of source ids.
                                 'source_run_ids': List of source run ids.
        :param breadcrum: Customizable field at start of run that contain runtime breadcrumb trail info/metadata or metadata
        for retry scenarios.
        :param specific_sources: A list of source ID or source run ids depending on value i dependency_check. Only the
                                 provided ids are matched (must be subset of associated sources or source run ids).
                                  If no matches found throws datasetDependencyException.

                                 Throws datasetDependencyException if can't start run given this
                                 dependency check rule.
        :param ext_job_run_key:
        :param ext_job_run_output_log_link:
        :param ext_etl_proc_key:
        :param ext_etl_proc_output_log_link:
        :raise: datasetAlreadyActiveException if dataset_observer_id already has an active or orphaned run process.
        :return: PdldatasetStartResult containing run_id and any available list of sources run metadata (PdldatasetQueue).
        """

        if (dependency_check == 'source_ids' or dependency_check == 'source_run_ids') and\
                (specific_sources is None or len(specific_sources) == 0):
            raise ConfigValidationException("Error: You did not provide any specific_sources where "
                                                    "specific_sources: %s" % (specific_sources))
        if (dependency_check not in ['source_ids','source_run_ids', 'ignore', 'any','all']):
            raise ConfigValidationException("Error: dependency_check argument not valid: "
                                                    "%s" % (dependency_check))

        run_id = get_new_guid().hex
        start_dt = datetime.now()

        try:

            # Start transaction from here and make repeatable read to make sure consistency
            # in case of multiple (un-authorized) sinks.
            conn.start_transaction()  # isolation_level='READ COMMITTED')

            # print("start dt {}".format(start_dt))

            # 1) Get number of source dependencies and other dep metadata.
            #

            # 2) Lookup all associated sources_dataset_run_ids runs and return rowcounts/dataset_batch_id
            #    and model keys for each source run IDs

            # 3) Update Queue that sink has started to run and use #2 source_run_ids

            # 4) Insert dataset run record (ready/start)
            #       -Some defensive code to prevent duplicates running for dataset ID

            # Dont worry, dependency_check ignore gets handled like 'any' during this call
            queue_list, unique_source_id_list, static_source_rel_count, sink_id, source_run_id_list, \
            orphan_sink, dataset_rel_id_list, sink_observer_config = \
                self.__internal_sources_ready_in_queue(conn, dataset_observer_id, dependency_check=dependency_check,
                                                       specific_sources=specific_sources)

            logger.info("queue_list: %s \n unique_source_id_list: %s \n static_source_rel_count: %d \n "
                  "unique_sink_id_list: %s \n orphan sink: %s" %
                  (len(queue_list), unique_source_id_list, static_source_rel_count, sink_id, orphan_sink))

            if orphan_sink == True:
                logger.warning("queue_list: %s \n unique_source_id_list: %s \n static_source_rel_count: %d \n "
                            "unique_sink_id_list: %s \n orphan sink: %s" %
                            (len(queue_list), unique_source_id_list, static_source_rel_count, sink_id, orphan_sink))
                raise AlreadyActiveException("Can't run dataset, there is one already active/orphaned")

            dataset_start_result = DatasetStartResult(run_id, sink_id, queue_list, static_source_rel_count,
                                                          unique_source_id_list,
                                                          source_run_id_list)

            if (dependency_check == 'source_ids' or dependency_check == 'source_run_ids') and \
                    len(unique_source_id_list) == 0:
                raise DependencyException("Error: No matching specific_sources found "
                                                  "specific_sources: %s, unique run sources: %s" % (specific_sources,
                                                                                                len(unique_source_id_list)))
            elif dependency_check == 'all' and static_source_rel_count > len(unique_source_id_list):
                raise DependencyException("Error: All dependencies must be present: static "
                                                   "source count: %s, unique run sources: %s" % (static_source_rel_count,
                                                                                                 len(unique_source_id_list)))
            elif dependency_check == 'any' and len(unique_source_id_list) == 0 and static_source_rel_count > 0:
                raise DependencyException("Error: Some dependencies must be present: static "
                                                   "source count: %s, unique run sources: %s" % (static_source_rel_count,
                                                                                                 len(unique_source_id_list)))
            else:
                pass  # must be 'ignore' or all input is valid

            # print("source_run_id_list")
            # print(source_run_id_list)

            ####
            # Now prepare the update to the queue

            q_update_cursor = conn.cursor()

            # source_run_id_list_str = ", ".join(source_run_id_list)
            # if sources_id_list is not None and len(sources_id_list) > 0:
            #    id_list_qmark = ', '.join('s' * len(sources_id_list)).replace('s', '%s')

            run_list_params=None
            if source_run_id_list is not None and len(source_run_id_list) > 0:
                run_list_params = ', '.join('s' * len(source_run_id_list)).replace('s', '%s')

            where_list = [run_id, start_dt, dataset_observer_id]
            stmt_update = """UPDATE dataset_source_sink_event_queue q
                               JOIN dataset_source_to_sink_meta_rel rel ON (q.dataset_rel_id = rel.dataset_rel_id)
                             SET q.sink_run_id=%s, q.sink_start_dt=%s WHERE """
            stmt_update += "q.sink_run_id = 'zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz' "
            stmt_update += 'AND rel.sink_dataset_id=%s '
            # if id_list_qmark is not None:
            #    stmt_update += 'AND source_dataset_id IN (%s) '.format(id_list_qmark)
            #    where_list.append(sources_id_list)

            if run_list_params is not None:
                stmt_update += 'AND q.source_run_id IN (%s)' % run_list_params
                where_list += source_run_id_list

            input_vals = where_list
            # print(input_vals)
            # print("stmt update {}".format(stmt_update))

            q_update_cursor.execute(stmt_update, input_vals)

            update_count=q_update_cursor.rowcount

            # print("update count: {}".format(update_count))

            # If this updates zero records when run_list_params is
            #  is provided there is a problem with what user is trying do.
            #  It can be zero if run_list_params is empty/None but if run_list was
            #  provided something should have been found. So will error out.
            if run_list_params is not None and update_count != len(source_run_id_list):
                q_update_cursor.close()
                conn.rollback()
                raise DatasetBaseException("Error: Source run ids provided, but no queue updates detected. "
                                            "Bad source run ids? : {}".format(source_run_id_list))
            q_update_cursor.close()

            ###

            # Insert new run record

            cursor = conn.cursor()

            col_name_list = []
            col_type_list = []
            col_val_list = []

            col_name_list.append('run_id')
            col_type_list.append("%s")
            col_val_list.append(run_id)

            col_name_list.append('dataset_observer_id')
            col_type_list.append("%s")
            col_val_list.append(dataset_observer_id)

            col_name_list.append('start_dt')
            col_type_list.append("%s")
            col_val_list.append(start_dt)

            if sink_observer_config is not None and len(sink_observer_config) > 0:
                col_name_list.append('run_observer_config')
                col_type_list.append("%s")
                col_val_list.append(sink_observer_config)

            if breadcrumb is not None:
                col_name_list.append('run_breadcrumb')
                col_type_list.append("%s")
                col_val_list.append(breadcrumb)

            if ext_job_run_key is not None:
                col_name_list.append('ext_job_run_key')
                col_type_list.append("%s")
                col_val_list.append(ext_job_run_key)

            if ext_job_run_output_log_link is not None:
                col_name_list.append('ext_job_run_output_log_link')
                col_type_list.append("%s")
                col_val_list.append(ext_job_run_output_log_link)

            if ext_etl_proc_key is not None:
                col_name_list.append('ext_etl_proc_key')
                col_type_list.append("%s")
                col_val_list.append(ext_etl_proc_key)

            if ext_etl_proc_output_log_link is not None:
                col_name_list.append('ext_etl_proc_output_log_link')
                col_type_list.append("%s")
                col_val_list.append(ext_etl_proc_output_log_link)

            col_list_str = ", ".join(col_name_list)
            col_type_list_str = ", ".join(col_type_list)

            stmt_insert = 'INSERT INTO dataset_observer_run (status, %s) VALUES (2, %s);' % (col_list_str, col_type_list_str)
            # print("stmt insert {}".format(stmt_insert))
            input_vals = col_val_list
            cursor.execute(stmt_insert, input_vals)
            cursor.close()

            # Defensive code to prevent multiple dataset IDs being active at the same time.

            # if another dataset_run is not final or error, then rollback this one
            query_cursor = conn.cursor()

            stmt_query = "SELECT run_id, status FROM dataset_observer_run" \
                         " WHERE dataset_observer_id = %s AND (status = 1 OR status = 2)"
            input_vals = (dataset_observer_id,)
            query_cursor.execute(stmt_query, input_vals)

            rows = query_cursor.fetchall()
            if query_cursor.rowcount > 1:
                query_cursor.close()
                conn.rollback()
                raise AlreadyActiveException("Defensive check: Can't run dataset, there is one already active/orphaned {}".format(rows))

            query_cursor.close()
            conn.commit()
            return dataset_start_result

        finally:
            if conn.in_transaction:
                conn.rollback()

    def test_dbcon(self):
        conn = self.__get_db_con()
        cursor = conn.cursor()
        query = "SELECT dataset_observer_id FROM dataset_observer limit 1"
        cursor.execute(query)
        cursor.fetchall()
        cursor.close()
        self.__close_db_con(conn)


if __name__ == '__main__':
    db_host: str = os.environ['DLC_DB_HOST']
    db_user:str = os.environ['DLC_DB_USER']
    db_password: str = os.environ['DLC_DB_PASS']
    db_name='pdl'

    print(uuid.uuid4())
    print(uuid.uuid1())
    print(uuid.uuid1())

    dataset_coord = DatasetLineage(db_host, db_user, db_password, db_name)

    dataset_coord.test_dbcon()

    # print(dataset_coord.create_dataset("sz", "billing", "key12", "key2"))

