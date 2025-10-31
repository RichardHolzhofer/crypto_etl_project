from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from src.constants import POSTGRES_CONN_ID


def create_table_for_coin(table_name:str):
    postgres_hook = PostgresHook(
        postgres_conn_id=POSTGRES_CONN_ID      
    )
    
    create_table_query = f"""
    
    CREATE TABLE IF NOT EXISTS {table_name} (
      record_time TIMESTAMP WITHOUT TIME ZONE PRIMARY KEY,
      open_price NUMERIC NOT NULL,
      high_price NUMERIC,
      low_price NUMERIC,
      close_price NUMERIC NOT NULL,
      change_pct NUMERIC,
      day_status VARCHAR(10),
      load_ts TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
      );
    
    """
    postgres_hook.run(create_table_query)
    

    