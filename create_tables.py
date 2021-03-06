#Drops and re-creates tables
#Dependencies:sql_queries.py, dwh.cfg

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """    
    for query in create_table_queries:        
        cur.execute(query)
        conn.commit()

        
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    """
    - Connect to the redshift with the configuration specified in dwh.cfg.
    - Returns the connection to the db on the cluster.
    """
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)


    conn.close()


if __name__ == "__main__":
    main()
