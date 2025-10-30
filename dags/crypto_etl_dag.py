from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import datetime
import json

#Creating DAG for weekly execution
with DAG(
    dag_id='crypto_etl_pipeline',
    start_date=datetime(2025,1,1),
    schedule='@weekly',
    catchup=False
) as dag:
    
    # Creating table in Postgres
    
    
    # Extracting Bitcoin prices for the last 7 days for every 4 hours
    
    #Transforming the data
    
    #Loading the data into Postgres
    
    #Defining the task dependencies
    
    #Checking the data in DBeaver