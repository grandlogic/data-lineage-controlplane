import unittest
import os
from data_lineage.dataset_lineage_mgmt import DatasetLineage
from data_lineage.dataset_lineage_mgmt import DBManager

import mysql.connector as dbapi_connector


def setUpModule():
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
                    DELETE FROM dataset_source_to_sink_meta_rel WHERE sink_dataset_id IN 
                        (SELECT dataset_observer_id FROM dataset_observer WHERE model_name like '%--test')
                   """
                   )
    cursor.execute("""
                    DELETE FROM dataset_source_sink_event_queue WHERE sink_run_id IN 
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


def tearDownModule():
    pass


class TestDatasetLineage(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db_host = os.environ['DSET_DB_HOST']
        cls.db_user = os.environ['DSET_DB_USER']
        cls.db_password = os.environ['DSET_DB_PASS']
        cls.db_name = os.environ['DSET_DB_NAME']

        cls.db_con = dbapi_connector.connect(
            host=cls.db_host,
            user=cls.db_user,
            passwd=cls.db_password,
            database=cls.db_name
        )
        cls.db_con.autocommit = True


    @classmethod
    def tearDownClass(cls):
        pass

    def test_1_declare_dataset_observer(self):
        cls = TestDatasetLineage()
        dataset_observer = DatasetLineage(db_con=cls.db_con)

        dataset1 = dataset_observer.declare_dataset_observer(model_name="some_dataset1--test")

        dataset2 = dataset_observer.declare_dataset_observer(model_name="another_dataset2--test",
                                                             model_namespace="somespace",
                                                             model_dataset_props="key stuff",
                                                             model_zone_tag=3, description="cool model",
                                                             observer_config="some config",
                                                             display_name='some disp name')
        print("Declare Dataset Observer")

    def test_2_0_update_dataset_observer(self):
        cls = TestDatasetLineage()
        dataset_observer = DatasetLineage(db_con=cls.db_con)

        dataset1 = dataset_observer.declare_dataset_observer(model_name="another_dataset3--test",
                                                             model_namespace="somespace",
                                                             model_dataset_props="key stuff",
                                                             model_zone_tag=3,
                                                             description="cool model")

        dataset_observer.update_dataset_observer(dataset1, description="changed description",
                                                 observer_config="config changes",
                                                 display_name='disp name')
        print("Update Dataset Observer")

    def test_2_1_update_dataset_observer_status(self):
        cls = TestDatasetLineage()
        dataset_observer = DatasetLineage(db_con=cls.db_con)

        dataset1 = dataset_observer.declare_dataset_observer(model_name="another_dataset3_1--test",
                                                             model_namespace="somespace",
                                                             model_dataset_props="key stuff",
                                                             model_zone_tag=3,
                                                             description="cool model")

        dataset_observer.update_dataset_observer_status(dataset1, observer_status=2)

        print("Update Dataset Observer Status Only")

    def test_3_associate_dataset_source_to_sink(self):
        cls = TestDatasetLineage()
        dataset_observer = DatasetLineage(db_con=cls.db_con)

        dataset1 = dataset_observer.declare_dataset_observer(model_name="some_dataset4--test")

        dataset2 = dataset_observer.declare_dataset_observer(model_name="another_dataset5--test",
                                                             model_namespace="somespace",
                                                             model_dataset_props="key stuff",
                                                             model_zone_tag=3,
                                                             description="cool model",
                                                             observer_config="some config")

        rel_id = dataset_observer.associate_dataset_source_to_sink(dataset1, dataset2)
        print("rel_id %s:" % rel_id)
        # second time should be ignored and id returned
        rel_id = dataset_observer.associate_dataset_source_to_sink(dataset1, dataset2)
        print("rel_id %s:" % rel_id)
        print("Associate 2 Dataset Observers source/sink")

    def test_4_0_disassociate_dataset_source_from_sink(self):
        cls = TestDatasetLineage()
        dataset_observer = DatasetLineage(db_con=cls.db_con)

        dataset1 = dataset_observer.declare_dataset_observer(model_name="some_dataset6--test")

        dataset2 = dataset_observer.declare_dataset_observer(model_name="another_dataset7--test",
                                                             model_namespace="somespace",
                                                             model_dataset_props="key stuff", model_zone_tag=3,
                                                             description="cool model",
                                                             observer_config="some config")

        rel_id = dataset_observer.associate_dataset_source_to_sink(dataset1, dataset2)
        print(rel_id)

        status = dataset_observer.disassociate_dataset_source_from_sink(dataset1, dataset2)
        print(status)
        status = dataset_observer.disassociate_dataset_source_from_sink(dataset1, dataset2)
        print(status)
        print("Disassociate 2 Dataset Observers source/sink")

    def test_4_1_disassociate_then_reassociate_dataset_source_from_sink(self):
        cls = TestDatasetLineage()
        dataset_observer = DatasetLineage(db_con=cls.db_con)

        dataset1 = dataset_observer.declare_dataset_observer(model_name="some_dataset8--test")

        dataset2 = dataset_observer.declare_dataset_observer(model_name="another_dataset9--test",
                                                             model_namespace="somespace",
                                                             model_dataset_props="key stuff",
                                                             model_zone_tag=3,
                                                             description="cool model",
                                                             observer_config="some config")

        dataset_observer.associate_dataset_source_to_sink(dataset1, dataset2)

        dataset_observer.disassociate_dataset_source_from_sink(dataset1, dataset2)

        dataset_observer.associate_dataset_source_to_sink(dataset1, dataset2)

        print("Disassociate 2 Dataset Observers source/sink, then re-associate them again")

    def test_5_0_start_dataset_observer_run_with_id(self):
        cls = TestDatasetLineage()
        #simple case

        dataset_observer = DatasetLineage(db_con=cls.db_con)

        dataset1 = dataset_observer.declare_dataset_observer(model_name="test5_0--test")

        result = dataset_observer.start_dataset_observer_run_with_id(dataset1)

    def test_5_1_finish_dataset_observer_run(self):
        cls = TestDatasetLineage()
        # simple case

        dataset_observer = DatasetLineage(db_con=cls.db_con)

        dataset1 = dataset_observer.declare_dataset_observer(model_name="test5_1--test")

        result = dataset_observer.start_dataset_observer_run_with_id(dataset1)
        dataset_observer.finish_dataset_observer_run(status="success", dataset_run_id=result.run_id)

    def test_6_0_fetch_ready_dataset_sources_by_sink_id(self):
        cls = TestDatasetLineage()
        dataset_observer = DatasetLineage(db_con=cls.db_con)

        dataset1 = dataset_observer.declare_dataset_observer(model_name="test6_0--test")

        dataset2 = dataset_observer.declare_dataset_observer(model_name="another_test6_0--test",
                                                             model_namespace="somespace",
                                                             model_dataset_props="key stuff",
                                                             model_zone_tag=3,
                                                             description="cool model",
                                                             observer_config="some config")

        dataset_observer.associate_dataset_source_to_sink(dataset1, dataset2)



        print("Disassociate 2 Dataset Observers source/sink, then re-associate them again")

    def test_x_joke(self):
        from data_lineage.dataset_lineage_mgmt import a_joke
        import time
        time.sleep(1) # just to get the test logging to not get interleaved with OK

        self.assertEqual(a_joke(), "I said something funny")


if __name__ == '__main__':
    unittest.main()