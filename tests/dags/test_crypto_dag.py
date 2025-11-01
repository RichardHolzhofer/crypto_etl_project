import os
import logging
from contextlib import contextmanager
import pytest
from airflow.models import DagBag
import importlib.util
from src.etl_logic import CoinGeckoPipeline


@contextmanager
def suppress_logging(namespace):
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value


def get_import_errors():
    """
    Generate a tuple for import errors in the dag bag
    """
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)

        def strip_path_prefix(path):
            return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))

        return [(None, None)] + [
            (strip_path_prefix(k), v.strip()) for k, v in dag_bag.import_errors.items()
        ]


def get_dags():
    """
    Generate a tuple of dag_id, <DAG objects> in the DagBag
    """
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)

    def strip_path_prefix(path):
        return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))

    return [(k, v, strip_path_prefix(v.fileloc)) for k, v in dag_bag.dags.items()]


@pytest.mark.parametrize(
    "rel_path,rv", get_import_errors(), ids=[x[0] for x in get_import_errors()]
)
def test_file_imports(rel_path, rv):
    """Test for import errors on a file"""
    if rel_path and rv:
        raise Exception(f"{rel_path} failed to import with message \n {rv}")


APPROVED_TAGS = {}


@pytest.mark.parametrize(
    "dag_id,dag,fileloc", get_dags(), ids=[x[2] for x in get_dags()]
)
def test_dag_tags(dag_id, dag, fileloc):
    """
    test if a DAG is tagged and if those TAGs are in the approved list
    """
    assert dag.tags, f"{dag_id} in {fileloc} has no tags"
    if APPROVED_TAGS:
        assert not set(dag.tags) - APPROVED_TAGS


@pytest.mark.parametrize(
    "dag_id,dag, fileloc", get_dags(), ids=[x[2] for x in get_dags()]
)
def test_dag_retries(dag_id, dag, fileloc):
    """
    test if a DAG has retries set
    """
    assert (
        dag.default_args.get("retries", None) >= 2
    ), f"{dag_id} in {fileloc} must have task retries >= 2."


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
    


def test_task_dependencies():
    dag_path = os.path.join(
        os.environ.get("AIRFLOW_HOME", "/usr/local/airflow"),
        "dags",
        "crypto_dag.py",
    )

    # Dynamically import the DAG module directly from file
    spec = importlib.util.spec_from_file_location("crypto_dag", dag_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Call the DAG factory function to get the actual DAG object
    dag = module.crypto_etl_pipeline()  # <-- note the parentheses!

    assert dag is not None, "DAG object not found or failed to initialize"

    # Validate task dependencies in-memory
    task = dag.get_task("extract_bitcoin_data")
    assert "create_table_for_bitcoin" in task.upstream_task_ids, (
        "extract_bitcoin_data should depend on create_table_for_bitcoin"
    )
    
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