# mysql_library.py
import mysql.connector
import pandas as pd
import getpass
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

def connect_to_db():
    try:
        password = getpass.getpass("Please enter your MySQL password: ")
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password=password,
        )
        print("Connection successful")
        return conn
    except Exception as e:
        print("Connection failed:", e)
        return None

def use_database(conn, db_name):
    try:
        cursor = conn.cursor()
        cursor.execute(f"USE {db_name}")
        print(f"Switched to database: {db_name}")
    except Exception as e:
        print("Failed to switch database:", e)

def select(conn, ar_query, verbose=True):
    # SHOW, DESCRIBE, SELECT
    try:
        cursor = conn.cursor()

        while cursor.nextset():
            cursor.fetchall()

        cursor.execute(ar_query)

        results = cursor.fetchall()
        if not results:
            if verbose:
                print("No results found")
            return pd.DataFrame()
        
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(data=results, columns=columns)
        df = df.where(pd.notnull(df), None)
        if verbose:
            print(f"Returned {len(df)} rows")
            display(df)
        return df
    except Exception as e:
        print("Query failed:", e)
        return pd.DataFrame()

def execute(conn, ar_query, verbose=True):
    try:
        cursor = conn.cursor()

        while cursor.nextset():
            cursor.fetchall()

        cursor.execute(ar_query)
        
        affected_rows = cursor.rowcount
        while cursor.nextset():
            cursor.fetchall()
            
        conn.commit()
        if verbose:
            print('Rows affected:', affected_rows)
    except Exception as e:
        print("Execution failed:", e)

def run(conn, ar_query):
    try:
        query_upper = ar_query.strip().upper()

        if any(query_upper.startswith(keyword) for keyword in ['SELECT', 'DESCRIBE', 'SHOW']):
            select(conn, ar_query)
        else:
            execute(conn, ar_query)
    except Exception as e:
        print('Error for running:', e)



def multi_execute(conn, ar_query, verbose=False):
    try:
        cursor = conn.cursor()
        queries = [q.strip() for q in ar_query.split(';') if q.strip()]
        
        for q in queries:
            
            # ensure no unread info before executing
            while cursor.nextset():
                cursor.fetchall()

            cursor.execute(q)
            affected_rows = cursor.rowcount

            # ensure no unread info after executing
            while cursor.nextset():
                cursor.fetchall()

            if verbose:
                print(f"Executed: {q}")
                print(f"Affected Rows: {affected_rows}\n")

        # commit after all the queries are done successfully 
        conn.commit()
        print("All commands executed successfully and committed")

    except Exception as e:
        # rollback to the beginning if any query does not work
        conn.rollback()
        print("Execution failed, transaction rolled back:", e)



def close_connection(conn):
    if conn:
        conn.close()
        print("Connection closed")
