from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum
class CoinGeckoPipeline:
  
  def __init__(self, conn_id:str):
    self.conn_id = conn_id

  @task.instance()
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
      
  @task.instance()
  def transform_ohlc_data(self, raw_data:list):
    transformed_data = []
    
    for coin_data in raw_data:
      record_time = pendulum.from_timestamp(coin_data[0] / 1000).isoformat()
      load_time = pendulum.now().isoformat()
      
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
      
    return transformed_data
  
  @task.instance()
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
    return result[0] if result else None
  
  @task.instance()
  def create_change_pct_and_day_status_col(self, transformed_data, last_close_price=None):
    
    for i, row in enumerate(transformed_data):
      if i == 0:
        if last_close_price is None:
          row['change_pct'] = None
          row['day_status'] = None
        else:
          row['change_pct'] = (
            (row["close_price"] - last_close_price) / last_close_price
            ) * 100
          if row["close_price"] < last_close_price:
            row['day_status'] = "Down"
          elif row["close_price"]  > last_close_price:
            row['day_status'] = "Up"
          else:
            row['day_status'] = 'Flat'
      else:
        previous_close_price = transformed_data[i-1]["close_price"]
        row['change_pct'] = ((row["close_price"] - previous_close_price) / previous_close_price) * 100
        
        if row["close_price"] < previous_close_price:
            row['day_status'] = "Down"
        elif row["close_price"]  > previous_close_price:
          row['day_status'] = "Up"
        else:
          row['day_status'] = 'Flat'
      
    return transformed_data

