import configparser
import utils
import sql_queries


def load_staging_tables(cur, conn, config):
    for query in sql_queries.get_copy_table_queries(config):
        cur.execute(query)
        conn.commit()
        print("Executed: {}".format(query))
    print("Loaded staging tables")


def insert_tables(cur, conn, config):
    for query in sql_queries.get_insert_table_queries(config):
        cur.execute(query)
        conn.commit()
        print("Executed: {}".format(query))
    print("Insert into star schema")


def main():
    config = utils.get_config('dwh.cfg')

    conn = utils.connect_to_database(config)
    cur = conn.cursor()

    load_staging_tables(cur, conn, config)
    insert_tables(cur, conn, config)

    conn.close()


if __name__ == "__main__":
    main()
