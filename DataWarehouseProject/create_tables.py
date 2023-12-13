import configparser
import utils
import sql_queries


def drop_tables(cur, conn):
    for query in sql_queries.get_drop_table_queries():
        cur.execute(query)
        conn.commit()
    print("Dropped tables")


def create_tables(cur, conn):
    for query in sql_queries.get_create_table_queries():
        cur.execute(query)
        conn.commit()
    print("Created tables")


def main():
    config = utils.get_config('dwh.cfg')

    conn = utils.connect_to_database(config)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
