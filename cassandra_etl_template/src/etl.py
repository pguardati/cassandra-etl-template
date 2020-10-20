import argparse
import sys
from cassandra.cluster import Cluster

from cassandra_etl_template.src import utils_processing, create_tables
from cassandra_etl_template.src import check_database


def parse_input(args):
    """Parse terminal-like arguments into a namespace"""
    parser = argparse.ArgumentParser(description="User-defined settings of the data loading into the database")
    parser.add_argument("path_data_raw", help="Directory of the raw csv files")
    parser.add_argument("path_data_processed", help="Directory of the raw csv files")
    parser.add_argument("--reset-tables", help="Drop and create from scratch all the tables", action="store_true")
    return parser.parse_args(args)


def main(args=None):
    """Process raw csv data and load them in cassandra.
    One dedicated table is used for each query"""
    # read terminal-like input
    if args is None:
        args = sys.argv[1:]
    args = parse_input(args)

    # transform
    file_processed = utils_processing.clean_csv_dataset(args.path_data_raw, args.path_data_processed)

    # create database and check all tables are empty
    create_tables.main()

    # initialise cassandra session
    cluster = Cluster()
    session = cluster.connect()
    session.set_keyspace('sparkifydb')

    # load data into tables
    utils_processing.etl_session_analysis(session, file_processed)
    utils_processing.etl_user_activity(session, file_processed)
    utils_processing.etl_song_preference(session, file_processed)
    check_database.fetch_content()

    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()

