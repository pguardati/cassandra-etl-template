from cassandra.cluster import Cluster

from cassandra_etl_template.src import sql_queries


def reset_database():
    """Drop current database and create an empty one"""
    try:
        # connect, drop, create
        cluster = Cluster()
        session = cluster.connect()
        session.execute("""DROP KEYSPACE IF EXISTS sparkifydb;""")
        session.execute("""CREATE KEYSPACE IF NOT EXISTS sparkifydb
        WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }""")
        session.set_keyspace('sparkifydb')
        session.shutdown()
        cluster.shutdown()

        # reconnect, return session
        cluster = Cluster()
        session = cluster.connect()
        session.set_keyspace('sparkifydb')
        return cluster, session

    except Exception as e:
        print(e)


def drop_tables(session):
    """Drops each table using the queries in `drop_table_queries` list."""
    for query in sql_queries.drop_table_queries:
        session.execute(query)


def create_tables(session):
    """Creates each table using the queries in `create_table_queries` list."""
    for query in sql_queries.create_table_queries:
        session.execute(query)


def main():
    """Drop database and create a new one, in particular:
    - Drops (if exists) and Creates the sparkify database.
    - Establishes connection with the sparkify database and gets
    cursor to it.
    - Drops all the tables.
    - Creates all tables needed.
    - Finally, closes the connection.
    """
    cluster, session = reset_database()

    drop_tables(session)
    create_tables(session)

    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()
