import unittest
import shutil
from cassandra.cluster import Cluster

from sparkify_cassandra_db.constants import DIR_DATA_TEST, DIR_DATA_PROCESSED_TEST
from sparkify_cassandra_db.src import etl, utils_processing, create_tables, check_database


class TestETL(unittest.TestCase):
    """Check that each ETL pipeline run on the test set without error"""
    def setUp(self):
        print("Clearing database and processing data..")
        create_tables.main()
        self.file_processed = utils_processing.clean_csv_dataset(DIR_DATA_TEST, DIR_DATA_PROCESSED_TEST)
        self.cluster = Cluster()
        self.session = self.cluster.connect()
        self.session.set_keyspace('sparkifydb')

    def tearDown(self):
        print("Checking database content and removing directory with processed data...")
        check_database.fetch_content()
        shutil.rmtree(DIR_DATA_PROCESSED_TEST)
        self.session.shutdown()
        self.cluster.shutdown()

    def test_query1(self):
        utils_processing.etl_session_analysis(self.session, self.file_processed)

    def test_query2(self):
        utils_processing.etl_user_activity(self.session, self.file_processed)

    def test_query3(self):
        utils_processing.etl_song_preference(self.session, self.file_processed)

    def test_pipeline(self):
        etl.main([DIR_DATA_TEST,DIR_DATA_PROCESSED_TEST])


if __name__ == "__main__":
    unittest.main()
