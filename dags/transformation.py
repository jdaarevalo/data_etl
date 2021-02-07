import os
import pendulum

from datetime import datetime, timedelta

from airflow import DAG
from airflow import settings
from airflow.models.connection import Connection
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator


def create_or_update_conn(conn_id, conn_type, login=None, password=None, host=None, port=None, schema=None):
    """         
    Create or update a connection in airiflow
                    
    Parameters      
    ----------      
    connection_id : str
        reference to a name of the connection
    conn_type : str     
        The connection type, could be postgres, mysql, aws, http ...
    login : str, optional
        The login, user for the connection
    password : str, optional
        The password for the connection
    host : str, optional
        The host for the connection
    port : str, optional
        The port for the connection
    schema : str, optional
        The schema for the connection

    Returns
    -------
    None
    """
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
    result = cursor.execute(query)
    cursor.close()
    conn.commit()

def get_records_from_query_in_postgres(connection_id, query):
    """         
    Execute in the database a operation from the query and get the results
                    
    Parameters      
    ----------      
    connection_id : str
        reference to a specific database
    query : str     
        string with sql command select  ... from ... where ...

    Returns
    -------
    result : str
        the result of the query
    """
    post_hook = PostgresHook(postgres_conn_id = connection_id)
    result = post_hook.get_records(query)
    return result

# Postgres 10.5 variables
#TODO get this variables from env with os.environ["POSTGRES_USER"]
POSTGRES_USER = 'postgres'
POSTGRES_PASSWORD = 'postgres'
POSTGRES_HOST = 'postgres'
POSTGRES_PORT = 54321
POSTGRES_SCHEMA = 'postgres'

CONNECTION_ID = 'POSTGRES_CONNECTION'

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

# query to get the top interesting
QUERY_TOP_MOST_INTERESTING = """select subreddit_id, max(score) max_score
                                from posts_2013​
                                where ups > 5000 
                                and downs > 5000
                                    and ups - downs < 10000
                                    group by 1
                                    order by max_score desc 
                                    limit 10;"""


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
                              op_kwargs={'conn_id': CONNECTION_ID,
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
                              op_kwargs={'connection_id': CONNECTION_ID,
                                  'query': QUERY_TO_INSERT_OR_UPDATE},
                              dag=dag)

t_execute_query_interest = PythonOperator(
                              task_id='execute_query_top_interesting',
                              python_callable=get_records_from_query_in_postgres,
                              op_kwargs={'connection_id': 'POSTGRES_CONNECTION',
                                  'query': QUERY_TOP_MOST_INTERESTING},
                              dag=dag)

_create_or_update_conn >> t_execute_query_postgres >> t_execute_query_interest
