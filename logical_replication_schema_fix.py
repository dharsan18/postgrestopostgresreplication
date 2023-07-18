import datetime
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
import logging

app_db = """SELECT
	table_schema ,
	table_name,
	column_name 
      FROM
	information_schema.columns
      WHERE
      table_schema = 'bigspring' and 
	table_name in (
	select 
	ppt.tablename 
	from 
	pg_catalog.pg_publication_tables ppt 
	)"""

dw_sql = """SELECT
	table_schema ,
	table_name,
	column_name 
      FROM
	information_schema.columns
      WHERE
      table_schema = 'bigspring' and
	table_name in (select relname
FROM pg_catalog.pg_statio_user_tables
join pg_catalog.pg_subscription_rel on relid = srrelid
	)"""

create_ddl = """select 
'DROP TABLE IF EXISTS ' || table_name || '; ' 
'CREATE TABLE ' || table_name || ' ('
|| column_names || '); '
from 
(select 
(table_schema ||'.' ||  table_name ) table_name , string_agg( column_name || ' ' || 
(case when data_type = 'USER-DEFINED' then 'text'
when data_type like '%json%' then 'text'
else data_type end )
, ',') column_names
from information_schema.columns
where table_name = '$$$$'
group by 1
) d"""

indexes_ddl = """select string_agg(replace ( replace ( pg_get_indexdef(format('%I.%I', schemaname, indexname)::regclass),
'UNIQUE','') , 'INDEX' , 'INDEX IF NOT EXISTS ') || ';','')
from pg_indexes
where schemaname not in ('pg_catalog', 'information_schema')
and tablename in ('$$$$')"""


def query_postgres():
      """
      Queries Postgres and returns a cursor to the results.
      """
      postgres_dw = PostgresHook(postgres_conn_id='datawarehouse_conn')
      conn_dw = postgres_dw.get_conn()
      cursor_dw = conn_dw.cursor()
      cursor_dw.execute(dw_sql)

      postgres_app = PostgresHook(postgres_conn_id='logical_replication_conn')
      conn_app = postgres_app.get_conn()
      cursor_app = conn_app.cursor()
      cursor_app.execute(app_db)

      dw_data = cursor_dw.fetchall()
      app_data = cursor_app.fetchall()
      unmatched_items_app = [d for d in app_data if d not in dw_data]
      unmatched_items_dw = [d for d in dw_data if d not in app_data]
      unmatched_items_app.extend(unmatched_items_dw)

      if unmatched_items_app :
            tables_to_recreate = set([d[:2] for d in unmatched_items_app])
            for table in tables_to_recreate :
                  # step 1 drop the publication for the table
                  cursor_app.execute('ALTER PUBLICATION my_publication drop table {0}.{1}'.format(table[0],table[1]))
                  # step 2 get the new ddl for the table
                  cursor_app.execute(create_ddl.replace('$$$$',table[1]))
                  table_ddl = cursor_app.fetchall()
                  # step 3 recreate table in destination
                  cursor_dw.execute(table_ddl[0][0])
                  # step 4 add table back to publication
                  cursor_app.execute('ALTER PUBLICATION my_publication add table {0}.{1}'.format(table[0],table[1]))
                  # step 5 add indexes back to the destination tables
                  cursor_app.execute(indexes_ddl.replace('$$$$',table[1]))
                  index_ddl = cursor_app.fetchall()
                  if index_ddl[0][0]:
                        cursor_dw.execute(index_ddl[0][0])
            conn_dw.commit()
            conn_app.commit()
            


with DAG(
    dag_id="logical_replication_schema_fix",
    start_date=datetime.datetime(2020, 2, 2), # Any Past Date
    schedule_interval="*/5 * * * *", # Run every 5 minutes
    catchup=False, # Should be False for generic Sql
) as dag:

  logical_replication_fix = PythonOperator(
    task_id="logical_replication_fix",
    python_callable=query_postgres,
    )
  
  alter_subscription = PostgresOperator(
        task_id='alter_subscription',
        sql='ALTER SUBSCRIPTION my_subscription REFRESH PUBLICATION',
        postgres_conn_id='datawarehouse_conn',
        autocommit=True
    )

 
  logical_replication_fix >> alter_subscription
                  
                  

