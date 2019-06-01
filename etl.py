import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    to load staging tables from S3 bucket
    :param cur: an instance to execute database commands
    :param conn: a connection object that creates a connection to database
    :return: none
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    to load fact and dimension tables from staging tables
    :param cur: an instance to execute database commands
    :param conn: a connection object that creates a connection to database
    :return: none
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    main method that calls load_staging_tables then insert_tables
    :return: none
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()