import os
from data_lineage.dataset_lineage_mgmt import DBManager
import mysql.connector as dbapi_connector


def reset_test_db():
    db_host = os.environ['DSET_DB_HOST']
    db_user = os.environ['DSET_DB_USER']
    db_password = os.environ['DSET_DB_PASS']
    db_name = os.environ['DSET_DB_NAME']

    db_con = dbapi_connector.connect(
        host=db_host,
        user=db_user,
        passwd=db_password,
        database=db_name
    )
    db_con.autocommit = True

    db_con.start_transaction()

    # db_con.start_transaction(consistent_snapshot=True, isolation_level='READ COMMITTED')

    cursor = db_con.cursor()

    cursor.execute("""
                            DELETE FROM dataset_observer_run WHERE dataset_observer_id IN 
                                (SELECT dataset_observer_id FROM dataset_observer WHERE model_name like '%--test')
                           """
                   )
    cursor.execute("""
                        DELETE FROM dataset_source_sink_event_queue WHERE dataset_rel_id IN 
                            (SELECT rel.dataset_rel_id 
                            FROM dataset_observer o
                              JOIN dataset_source_to_sink_meta_rel rel ON (rel.source_dataset_id = o.dataset_observer_id)
                            WHERE model_name like '%--test')
                        """
                   )
    cursor.execute("""
                            DELETE FROM dataset_source_sink_event_queue WHERE dataset_rel_id IN 
                            (SELECT rel.dataset_rel_id 
                            FROM dataset_observer o
                              JOIN dataset_source_to_sink_meta_rel rel ON (rel.sink_dataset_id = o.dataset_observer_id)
                            WHERE model_name like '%--test')
                            """
                   )
    cursor.execute("""
                        DELETE FROM dataset_source_to_sink_meta_rel WHERE source_dataset_id IN 
                            (SELECT dataset_observer_id FROM dataset_observer WHERE model_name like '%--test')
                       """
                   )
    cursor.execute("""
                        DELETE FROM dataset_source_to_sink_meta_rel WHERE sink_dataset_id IN 
                            (SELECT dataset_observer_id FROM dataset_observer WHERE model_name like '%--test')
                       """
                   )

    select_sql = "SELECT dataset_observer_id FROM dataset_observer WHERE model_name like '%--test'"
    cursor.execute(select_sql)

    rows = cursor.fetchall()
    dataflow_id_list = []
    for row in rows:
        dataflow_id_list.append(row[0])

    # print("Check if dataset to delete")
    if len(dataflow_id_list) > 0:
        item_list = ', '.join(["'%s'" % item for item in dataflow_id_list])
        #print("Ready to delete dataflow")

    for item in dataflow_id_list:
        del_query = "DELETE FROM dataset_observer WHERE dataset_observer_id = '" + item + "'"
        # print(del_query)
        cursor.execute(del_query)

    db_con.commit()
    print("Conditional TRUNCATE OF TEST DB TABLES/ROWS")

    cursor.close()
    db_con.close()