from airflow.providers.postgres.hooks.postgres import PostgresHook
from decimal import Decimal
import pendulum
class CoinGeckoPipeline:
  
  def __init__(self, conn_id:str):
    self.conn_id = conn_id

  def create_table_for_coin(self, table_name:str):
      postgres_hook = PostgresHook(
          postgres_conn_id=self.conn_id     
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
      print(f"Created table: {table_name} in {self.conn_id}")
      
  def transform_ohlc_data(self, raw_data:list):
    transformed_data = []
    
    for coin_data in raw_data:
      record_time = pendulum.from_timestamp(coin_data[0] / 1000).isoformat()
      load_time = pendulum.now('UTC').isoformat()
      
      transformed_data.append(
        {
          "record_time": record_time,
          "open_price":coin_data[1],
          "high_price":coin_data[2],
          "low_price":coin_data[3],
          "close_price":coin_data[4],
          "load_ts":load_time
        }
      )
    print("Data transformation completed.") 
    return transformed_data
  
  def get_last_close_price(self, table_name:str):
    
    postgres_hook = PostgresHook(
      postgres_conn_id=self.conn_id     
      )
    
    last_close_price_query = f"""
    SELECT
      close_price
    FROM {table_name}
    ORDER BY record_time DESC
    LIMIT 1    
    """
    
    result = postgres_hook.get_first(last_close_price_query)
    print(f"Identified last closed price in: {table_name} in {self.conn_id}")
    return result[0] if result else None
  
  def create_change_pct_and_day_status_col(self, transformed_data, last_close_price=None):
    
    for i, row in enumerate(transformed_data):
      close_price = Decimal(str(row['close_price']))
      
      if i == 0:
        if last_close_price is None:
          row['change_pct'] = None
          row['day_status'] = None
        else:
          row['change_pct'] = (
            (close_price - last_close_price) / last_close_price
            ) * 100
          if close_price < last_close_price:
            row['day_status'] = "Down"
          elif close_price  > last_close_price:
            row['day_status'] = "Up"
          else:
            row['day_status'] = 'Flat'
      else:
        previous_close_price = Decimal(str(transformed_data[i-1]["close_price"]))
        row['change_pct'] = ((close_price - previous_close_price) / previous_close_price) * 100
        
        if close_price < previous_close_price:
            row['day_status'] = "Down"
        elif close_price  > previous_close_price:
          row['day_status'] = "Up"
        else:
          row['day_status'] = 'Flat'
    print("Created 'change_pct' and 'day_status' columns.")  
    return transformed_data

  def load_data_into_postgres(self, table_name:str, transformed_data: list):
    postgres_hook = PostgresHook(
      postgres_conn_id=self.conn_id     
      )
    columns = ['record_time',
               'open_price',
               'high_price',
               'low_price',
               'close_price',
               'change_pct',
               'day_status',
               'load_ts'
               ]
    
    rows = [tuple(row[col] for col in columns) for row in transformed_data]
    
    postgres_hook.insert_rows(
      table=table_name,
      rows=rows,
      target_fields=columns,
      replace=False
    )
    print(f"Loaded transformed data into {table_name}")