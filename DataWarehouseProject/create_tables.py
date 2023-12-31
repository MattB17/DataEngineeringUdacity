"""
A script to create tables in the Redshift cluster for the Sparkify data
warehouse.

Before creating the tables, drop statements are executed in case the tables
already exist.

"""

import os
import configparser
import utils
import sql_queries


def drop_tables(cur, conn):
    """
    Drops any staging or production tables for Sparkify.

    This is a pre-processing step to ensure the database is purged of the
    tables before creation.

    Parameters
    ----------
    cur: The cursor to the Redshift database.
    conn: The connection to the Redshift database.

    Returns
    -------
    None

    """
    print("Dropping tables")
    for query in sql_queries.get_drop_table_queries():
        cur.execute(query)
        conn.commit()
    print("Tables dropped")


def create_tables(cur, conn):
    """
    Creates the staging and production tables for the Sparkify data warehouse.

    Parameters
    ----------
    cur: The cursor to the Redshift database.
    conn: The connection to the Redshift database.

    Returns
    -------
    None

    """
    print("Creating tables")
    for query in sql_queries.get_create_table_queries():
        cur.execute(query)
        conn.commit()
    print("Tables created")


def main():
    config = utils.get_config('dwh.cfg')

    # Create the cluster if it is not already setup.
    try:
        config.get('CLUSTER', 'endpoint')
    except:
        print("Running cluster setup script first")
        print("After running etl.py please run cluster_teardown.py to remove the cluster")
        print("The full workflow is in SparkifyPipeline.ipynb")
        os.system('python3 cluster_setup.py')
        config = utils.get_config('dwh.cfg')

    conn = utils.connect_to_database(config)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
