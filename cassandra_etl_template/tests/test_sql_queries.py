import unittest
from cassandra.cluster import Cluster

from cassandra_etl_template.src import create_tables, sql_queries


class TestQueries(unittest.TestCase):
    """Test life cycle (creation, insertion, query, deletion) of each query"""

    def setUp(self):
        create_tables.reset_database()
        self.cluster = Cluster()
        self.session = self.cluster.connect()
        self.session.set_keyspace('sparkifydb')

    def tearDown(self):
        self.cluster.shutdown()
        self.session.shutdown()

    def test_query1(self):
        self.session.execute(sql_queries.session_analysis_create)
        self.session.execute(sql_queries.session_analysis_insert,
                             (338, 4, "bad lip readings", "seagulls", 300.00))
        rows = self.session.execute(sql_queries.session_analysis_query, (338, 4))
        self.session.execute(sql_queries.session_analysis_drop)
        self.assertEqual(rows.column_names, ['artist_name', 'song_title', 'song_length'])
        self.assertEqual(list(rows.one()), ['bad lip readings', 'seagulls', 300.0])

    def test_query2(self):
        self.session.execute(sql_queries.user_activity_create)
        self.session.execute(sql_queries.user_activity_insert,
                             (10, 182, 1, "bad lip readings", "seagulls", "lillo", "barillo"))
        rows = self.session.execute(sql_queries.user_activity_query, (10, 182))
        self.session.execute(sql_queries.user_activity_drop)
        self.assertEqual(rows.column_names, ['artist_name', 'song_title', 'user_first_name', 'user_last_name'])
        self.assertEqual(list(rows.one()), ["bad lip readings", "seagulls", "lillo", "barillo"])

    def test_query3(self):
        self.session.execute(sql_queries.song_preference_create)
        self.session.execute(sql_queries.song_preference_insert,
                             ("All Hands Against His Own", 4, "barillo", "lillo",))
        rows = self.session.execute(sql_queries.song_preference_query, ('All Hands Against His Own',))
        self.session.execute(sql_queries.song_preference_drop)
        self.assertEqual(rows.column_names, ['user_last_name', 'user_first_name'])
        self.assertEqual(list(rows.one()), ["barillo", "lillo"])


if __name__ == "__main__":
    unittest.main()
