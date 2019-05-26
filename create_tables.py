import configparser
import psycopg2
from sql_queries import create_staging_table_queries, drop_staging_table_queries, create_dwh_table_queries, drop_dwh_table_queries


def drop_tables(cur, conn, tables_list):
    for query in tables_list:
        print(query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn, tables_list):
    for query in tables_list:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn, drop_staging_table_queries)
    create_tables(cur, conn, create_staging_table_queries)

    drop_tables(cur, conn, drop_dwh_table_queries)
    create_tables(cur, conn, create_dwh_table_queries)

    conn.close()


if __name__ == "__main__":
    main()