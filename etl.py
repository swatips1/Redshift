#Inserts data from S3 into the staging tables. Then inserts data from staging tables into the final tables.
#Dependencies:sql_queries.py, dwh.cfg

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

"""Load data into staging tables

Keyword arguments:
cur: Already established cursor object.
conn: Already established connection object.

"""

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()

"""Load data from staging tables to the final tables. 

Keyword arguments:
cur: Already established cursor object.
conn: Already established connection object.

"""
def insert_tables(cur, conn):
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


"""Entry point.
Loads and parses configuration file.
Establishes a connection with the database and aquires a cursor.
Calls function to load data into staging table and then into final tables.
Closes connections. 
"""
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print(config['CLUSTER'].values())
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
