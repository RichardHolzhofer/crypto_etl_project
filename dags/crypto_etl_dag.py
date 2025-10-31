from airflow import DAG
from src.etl_logic import CoinGeckoPipeline
import datetime
from airflow.providers.http.operators.http import HttpOperator
import json

from src.constants import (
    POSTGRES_CONN_ID,
    POSTGRES_BITCOIN_TABLE_NAME,
    POSTGRES_ETHEREUM_TABLE_NAME
)

#Creating DAG for weekly execution
with DAG(
    dag_id='crypto_etl_pipeline',
    start_date=datetime(2025,1,1),
    schedule='@weekly',
    catchup=False
) as dag:
    
    # Creating tables in Postgres
    
    pipeline = CoinGeckoPipeline(conn_id=POSTGRES_CONN_ID)
    
    create_bitcoin_table = pipeline.create_table_for_coin(table_name=POSTGRES_BITCOIN_TABLE_NAME)
    
    create_ethereum_table = pipeline.create_table_for_coin(table_name=POSTGRES_ETHEREUM_TABLE_NAME)
    
    # Extracting Bitcoin prices for the last 7 days for every 4 hours
    extract_bitcoin_data = HttpOperator(
        task_id='extract_coin_data',
        http_conn_id='coingecko_api',
        endpoint='bitcoin/ohlc',
        method='GET',
        data={
            "vs_currency": 'eur',
            "days": 7,
            "precision": 3
            },
        response_filter=lambda response: response.json(),
        log_response=True
    )
    
    extract_ethereum_data = HttpOperator(
        task_id='extract_coin_data',
        http_conn_id='coingecko_api',
        endpoint='ethereum/ohlc',
        method='GET',
        data={
            "vs_currency": 'eur',
            "days": 7,
            "precision": 3
            },
        response_filter=lambda response: response.json(),
        log_response=True
    )
    #Transforming the data
    
    #Loading the data into Postgres
    
    #Defining the task dependencies
    
    #Checking the data in DBeaver