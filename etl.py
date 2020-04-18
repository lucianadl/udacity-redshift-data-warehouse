import configparser
import psycopg2
from datetime import datetime
from sql_queries import set_schema_query, copy_table_queries, insert_table_queries

def set_schema(cur, conn):
    """
    Sets the database schema.
    
    Parameters:
    cur: Database connection cursor
    conn: Database connection
    """
    cur.execute(set_schema_query)
    
def load_staging_tables(cur, conn):
    """
    Executes all COPY commands defined in the list copy_table_queries (module sql_queries.py),
    loading the staging tables.
    
    Parameters:
    cur: Database connection cursor
    conn: Database connection
    """ 
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Executes all INSERT commands defined in the list insert_table_queries (module sql_queries.py),
    inserting data into the dimension and fact tables.
    
    Parameters:
    cur: Database connection cursor
    conn: Database connection
    """     
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    startTime = datetime.now()
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    set_schema(cur, conn)
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()

    print('Elapsed time: {}'.format(datetime.now() - startTime))

    
if __name__ == "__main__":
    main()