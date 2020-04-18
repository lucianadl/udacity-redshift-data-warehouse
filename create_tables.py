import configparser
import psycopg2
from datetime import datetime
from sql_queries import create_schema_query, set_schema_query, create_table_queries, drop_table_queries

def create_schema(cur, conn):
    """
    Drops (if exists) and creates the database schema.
    
    Parameters:
    cur: Database connection cursor
    conn: Database connection
    """
    cur.execute(create_schema_query)

def set_schema(cur, conn):
    """
    Sets the database schema.
    
    Parameters:
    cur: Database connection cursor
    conn: Database connection
    """    
    cur.execute(set_schema_query)

def drop_tables(cur, conn):
    """
    Executes all DROP TABLE queries defined in the list drop_table_queries (module sql_queries.py).
    
    Parameters:
    cur: Database connection cursor
    conn: Database connection
    """    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """
    Executes all CREATE TABLE queries defined in the list drop_table_queries (module sql_queries.py).
    
    Parameters:
    cur: Database connection cursor
    conn: Database connection
    """      
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    startTime = datetime.now()
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    create_schema(cur, conn)
    set_schema(cur, conn)
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

    print('Elapsed time: {}'.format(datetime.now() - startTime))
    
if __name__ == "__main__":
    main()