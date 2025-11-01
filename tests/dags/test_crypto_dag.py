import os
from airflow.models import DagBag
import importlib.util
from src.etl_logic import CoinGeckoPipeline



def tag_dag_id_and_tasks():
    dagbag = DagBag(include_examples=False)
    dag = dagbag.get_dag("crypto_etl_pipeline")
    assert dag is not None
    expected_task_ids = {
      "create_table_for_bitcoin",
      "create_table_for_ethereum",
      "extract_bitcoin_data",
      "extract_ethereum_data",
      "transform_ohlc_data",
      "get_last_close_price",
      "create_pct_and_day_status_cols",
      "load_data_into_postgres"
    }
    assert expected_task_ids.issubset(set(dag.task_ids))
    
def test_transform_ohlc_data():
    raw_data = [
    [
        1761408000000,
        95993.197,
        96245.086,
        95856.124,
        95873.262
    ],
    [
        1761422400000,
        95839.482,
        96078.821,
        95668.146,
        95989.185
    ],
    ]
    pipeline = CoinGeckoPipeline(conn_id="test_conn")
    result = pipeline.transform_ohlc_data(raw_data)
    
    assert len(result) == 2
    for record in result:
        assert "record_time" in record
        assert isinstance(record['record_time'], str)
        assert record['open_price'] >= 0
        assert record['close_price'] >= 0
        

def test_create_change_pct_and_day_status_col():
    test_data = [
        {"close_price": 50},
        {"close_price": 55},
        {"close_price": 53}
    ]
    last_close = 48
    pipeline = CoinGeckoPipeline(conn_id="test_conn")
    result = pipeline.create_change_pct_and_day_status_col(test_data, last_close)
    
    assert result[0]["day_status"] == "Up"
    assert "change_pct" in result[0]
    assert result[1]["day_status"] in ["Up", "Down", "Flat"]