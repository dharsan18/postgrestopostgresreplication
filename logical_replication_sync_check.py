import datetime
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator


def query_postgres():
      """
      Queries Postgres and returns a cursor to the results.
      """
      postgres_dw = PostgresHook(postgres_conn_id='datawarehouse_conn')
      conn_dw = postgres_dw.get_conn()
      cursor_dw = conn_dw.cursor()
      cursor_dw.execute("""SELECT count(*) FROM pg_catalog.pg_stat_subscription where subname = 'my_subscription'""")
      if cursor_dw.fetchall()[0][0] != 1 :
            raise ValueError('logical replication needs patching!')

      postgres_app = PostgresHook(postgres_conn_id='logical_replication_conn')
      conn_app = postgres_app.get_conn()
      cursor_app = conn_app.cursor()
      cursor_app.execute("""select case when state = 'streaming' then 'good' else 'fix it' end  from pg_stat_replication where application_name ='my_subscription'""")
      if cursor_app.fetchall()[0][0] != 'good' :
            raise ValueError('logical replication needs patching!')
      cursor_app.execute("""select active from pg_replication_slots where slot_name = 'my_subscription'""")
      if cursor_app.fetchall()[0][0] != True :
            raise ValueError('logical replication needs patching!')
      

with DAG(
    dag_id="logical_replication_sync_check",
    start_date=datetime.datetime(2020, 2, 2), # Any Past Date
    schedule_interval="0 */4 * * *", # Run every 4 hours once
    catchup=False, # Should be False for generic Sql
) as dag:

  replication_check = PythonOperator(
    task_id="replication_check",
    python_callable=query_postgres,
    )
 
  replication_check