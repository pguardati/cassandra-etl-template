import unittest

from sparkify_cassandra_db.src.etl import parse_input


class TestInterface(unittest.TestCase):
    """Check possible configurations of the etl inputs"""

    def test_configuration_to_insert_from_scratch(self):
        args = parse_input(["event_data", "event_data_processed", "--reset-tables"])
        self.assertEqual(args.path_data_raw, "event_data")
        self.assertEqual(args.path_data_processed, "event_data_processed")
        self.assertTrue(args.reset_tables)


if __name__ == "__main__":
    unittest.main()
