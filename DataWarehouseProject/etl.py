"""
A script to migrate data from S3 to staging tables and then to a final star
schema for the Sparkify data warehouse.

"""

import configparser
import utils
import sql_queries


def load_staging_tables(cur, conn, config):
    """
    Loads the raw data from S3 into staging tables.

    Parameters
    ----------
    cur: The cursor to the Redshift database.
    conn: The connection to the Redshift database.
    config: The set of configuration parameters needed to formulate the load
            statements.

    Returns
    -------
    None

    """
    print("Loading data into staging tables")
    for query in sql_queries.get_copy_table_queries(config):
        cur.execute(query)
        conn.commit()
        print("Executed: {}".format(query))
    print("Staging tables loaded")


def insert_tables(cur, conn, config):
    """
    Inserts data into the fact and dimension tables from the staging tables.

    Parameters
    ----------
    cur: The cursor to the Redshift database.
    conn: The connection to the Redshift database.
    config: The set of configuration parameters needed to formulate the load
            statements.

    Returns
    -------
    None

    """
    print("Inserting into star schema")
    for query in sql_queries.get_insert_table_queries(config):
        cur.execute(query)
        conn.commit()
        print("Executed: {}".format(query))
    print("Star schema populated")


def main():
    config = utils.get_config('dwh.cfg')

    conn = utils.connect_to_database(config)
    cur = conn.cursor()

    load_staging_tables(cur, conn, config)
    insert_tables(cur, conn, config)

    conn.close()


if __name__ == "__main__":
    main()
