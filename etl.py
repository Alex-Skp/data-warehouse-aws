import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function loops over the queries written in sql_queries.py which copies the content of a S3 Bucket 
    into our staging tables. Also prints in console the progress in every step of the loop.
    Args: 
        cur: Cursor for the connection, conn.cur() in a psycop2 connect object 
        conn: Psycop2.connect(connection string)
    Returns: Nothing 
    
    """
    for query in copy_table_queries:
        print("Running"+query[:40])
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function loops over the queries written in sql_queries.py which extracts data from the staging tables, transforms it and inserts it in 
    our star schema tables. Also prints in console the progress in every step of the loop.
    Args: 
        cur: Cursor for the connection, conn.cur() in a psycop2 connect object 
        conn: Psycop2.connect(connection string)
    Returns: Nothing 
    
    """
    
    for query in insert_table_queries:
        print("Running"+query[:40])
        cur.execute(query)
        conn.commit()


def main():
    """
    Loads config files, creates connection and cursor object to our S3 bucket, 
    Loads data from S3, and then inserts the data in the different tables of the star schema. 
    Closes the connection in the end
    
    """
    config = configparser.ConfigParser()
    config.read('credentials/dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)   
    insert_tables(cur, conn)

    conn.close()


#Executing main()
if __name__ == "__main__":
    main()