from airflow.decorators import dag, task
from src.etl_logic import CoinGeckoPipeline
import pendulum
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.email import EmailOperator

from src.constants import (
    POSTGRES_CONN_ID,
    POSTGRES_BITCOIN_TABLE_NAME,
    POSTGRES_ETHEREUM_TABLE_NAME
)

#Creating DAG for weekly execution
@dag(
    start_date=pendulum.datetime(2025,10,1),
    schedule='@weekly',
    catchup=False
)
def crypto_etl_pipeline():

    pipeline = CoinGeckoPipeline(conn_id=POSTGRES_CONN_ID)
    
    # Creating tables in Postgres
    @task
    def create_table_for_bitcoin():
        pipeline.create_table_for_coin(table_name=POSTGRES_BITCOIN_TABLE_NAME)

    @task
    def create_table_for_ethereum():     
        pipeline.create_table_for_coin(table_name=POSTGRES_ETHEREUM_TABLE_NAME)
    
    # Extracting Bitcoin prices for the last 7 days for every 4 hours
    extract_bitcoin_data = HttpOperator(
        task_id='extract_bitcoin_data',
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
        task_id='extract_ethereum_data',
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
    @task
    def transform_ohlc_data(raw_data):
        return pipeline.transform_ohlc_data(raw_data)
    
    
    #Getting last close prices    
    @task
    def get_last_close_price(table_name):
        return pipeline.get_last_close_price(table_name)
    
    
    #Create 'chage_pct' and 'day_status' columns    
    @task
    def create_pct_and_day_status_cols(transformed_data, last_close_price):
        
        return pipeline.create_change_pct_and_day_status_col(transformed_data, last_close_price)
    

    #Loading the data into Postgres
    @task
    def load_data_into_postgres(table_name, transformed_data):
        pipeline.load_data_into_postgres(table_name,transformed_data)
        
    
    ## Bitcoin flow
    create_table_for_bitcoin_task = create_table_for_bitcoin()
    bitcoin_raw = extract_bitcoin_data.output
    bitcoin_transformed = transform_ohlc_data(bitcoin_raw)
    bitcoin_last_close = get_last_close_price(POSTGRES_BITCOIN_TABLE_NAME)
    bitcoin_with_pct = create_pct_and_day_status_cols(bitcoin_transformed, bitcoin_last_close)
    load_bitcoin = load_data_into_postgres(POSTGRES_BITCOIN_TABLE_NAME, bitcoin_with_pct)
    
    ## Ethereum flow
    create_table_for_ethereum_task = create_table_for_ethereum()
    ethereum_raw = extract_ethereum_data.output
    ethereum_transformed = transform_ohlc_data(ethereum_raw)
    ethereum_last_close = get_last_close_price(POSTGRES_ETHEREUM_TABLE_NAME)
    ethereum_with_pct = create_pct_and_day_status_cols(ethereum_transformed, ethereum_last_close)
    load_ethereum = load_data_into_postgres(POSTGRES_ETHEREUM_TABLE_NAME, ethereum_with_pct)
    
    
    #Defining the task dependencies
    create_table_for_bitcoin_task >> extract_bitcoin_data >> bitcoin_transformed >> bitcoin_last_close >> bitcoin_with_pct >> load_bitcoin
    create_table_for_ethereum_task >> extract_ethereum_data >> ethereum_transformed >> ethereum_last_close >> ethereum_with_pct >> load_ethereum
    
dag = crypto_etl_pipeline()


