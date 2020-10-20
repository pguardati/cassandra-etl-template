import pandas as pd
from cassandra.cluster import Cluster

from cassandra_etl_template.src import sql_queries


def fetch_content():
    """Check partial content for each table in the database"""
    # open connection
    cluster = Cluster()
    session = cluster.connect()
    session.set_keyspace('sparkifydb')

    # print top elements of each table
    for i, table in enumerate(sql_queries.table_names):
        try:
            print("\n-- Table {}: {}\n".format(i + 1, table))
            rows = session.execute("SELECT * FROM {} LIMIT 5;".format(table))
            df = pd.DataFrame(rows, columns=rows.column_names)
            with pd.option_context('display.max_columns', None):
                print(df)
        except Exception as e:
            print(e)

    # close connection
    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    fetch_content()
