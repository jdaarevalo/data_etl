import os
import pendulum

from datetime import datetime, timedelta

from airflow import settings
from airflow.models.connection import Connection

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator


def create_or_update_conn(conn_id, conn_type, login=None, password=None, host=None, port=None, schema=None):
    session = settings.Session()
    try:
        connection_query = session.query(Connection).filter(Connection.conn_id == conn_id)
        connection_query_result = connection_query.one_or_none()
        if not connection_query_result:
            connection = Connection(conn_id=conn_id, conn_type=conn_type, login=login,
                                    password=password, host=host, port=port, schema=schema)
            session.add(connection)
            session.commit()
        else:
            connection_query_result.login = login
            connection_query_result.host = host
            connection_query_result.schema = schema
            connection_query_result.port = port
            connection_query_result.set_password(password)
            session.add(connection_query_result)
            session.commit()
    except Exception as e:
        #logger.info("Failed creating connection")
        #logger.info(e) 
        print("Failed creating connection" + e)

def execute_query_in_postgres(connection_id, query):
    """         
    Execute in the database a operation from the query.
    The method returns None. If the query was executed,
                    
    Parameters      
    ----------      
    connection_id : str
        reference to a specific database
    query : str     
        string with sql command select / update / delete/ ... from ... where ...

    Returns
    -------
    None
    """
    post_hook = PostgresHook(postgres_conn_id = connection_id)
    conn = post_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(query)
    cursor.close()
    conn.commit()

# Postgres 10.5 variables
#TODO get this variables from env with os.environ["POSTGRES_USER"]
POSTGRES_USER='postgres'
POSTGRES_PASSWORD='postgres'
POSTGRES_HOST= 'postgres'
POSTGRES_PORT= 54321
POSTGRES_SCHEMA='postgres'

# query to update values in postgres

QUERY_TO_INSERT_OR_UPDATE = """insert into posts_2013​
                               (select created_utc, score, ups, downs, permalink, id, subreddit_id
                               from data_challenge
                               where created_utc > '2013-01-01'
                               order by created_utc desc limit 100)
                               ON CONFLICT (id) DO UPDATE
                               SET created_utc = excluded.created_utc
                                  , score = excluded.score
                                  , ups = excluded.ups
                                  , downs = excluded.downs
                                  , permalink = excluded.permalink
                                  , subreddit_id = excluded.subreddit_id;"""

QUERY_TOP_MOST_INTERESTING = """ """
default_args = {
    'owner': 'data-challenge',
    'start_date': datetime(2021, 1, 5, tzinfo=pendulum.timezone("Europe/Berlin")),
    'retries': 2,
    'retry_delay': timedelta(minutes=3)
}

dag = DAG("transformation", default_args=default_args, schedule_interval='00 1 * * *', catchup=False)

t_create_or_update_conn = PythonOperator(
                              task_id='update_create_postgres_connection',
                              python_callable=create_or_update_conn,
                              op_kwargs={'conn_id': 'POSTGRES_CONNECTION',
                                  'conn_type': 'postgres',
                                  'login' : POSTGRES_USER,
                                  'password' : POSTGRES_PASSWORD,
                                  'host' : POSTGRES_HOST,
                                  'port' : POSTGRES_PORT,
                                  'schema' : POSTGRES_SCHEMA},
                              dag=dag)


t_execute_query_postgres = PythonOperator(
                              task_id='execute_query_in_postgres',
                              python_callable=execute_query_in_postgres,
                              op_kwargs={'connection_id': 'POSTGRES_CONNECTION',
                                  'query': QUERY_TO_INSERT_OR_UPDATE},
                              dag=dag)

t_create_or_update_conn >> t_execute_query_postgres
