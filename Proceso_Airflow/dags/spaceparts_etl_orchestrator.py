from __future__ import annotations
import logging
from datetime import timedelta
import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from notebooks.bronze_processor import BronzeProcessor
from notebooks.silver_processor import SilverProcessor
from notebooks.gold_processor import GoldProcessor

logger = logging.getLogger(__name__)
TZ = pendulum.timezone("America/Bogota")

@dag(
    dag_id="spaceparts_etl_orchestrator",
    description="Pipeline SpaceParts Bronze → Silver → Gold",
    schedule_interval="0 7 * * *",
    start_date=pendulum.datetime(2025, 9, 1, 7, 0, tz=TZ),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-eng", "retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["spaceparts", "bronze", "silver", "gold"],
)
def spaceparts_etl_orchestrator():
    @task(task_id="bronze_layer", retries=1, retry_delay=timedelta(minutes=5))
    def run_bronze() -> dict:
        ds = get_current_context().get("ds")
        logger.info("BRONZE | ds=%s", ds)
        return BronzeProcessor().execute_bronze_pipeline()

    @task(task_id="silver_layer", retries=1, retry_delay=timedelta(minutes=5))
    def run_silver(bronze_results: dict) -> dict:
        ds = get_current_context().get("ds")
        logger.info("SILVER | ds=%s", ds)
        return SilverProcessor().execute_silver_pipeline(bronze_results or {})

    @task(task_id="gold_layer", retries=1, retry_delay=timedelta(minutes=5))
    def run_gold(silver_results: dict) -> dict:
        ds = get_current_context().get("ds")
        logger.info("GOLD | ds=%s", ds)
        return GoldProcessor().execute_gold_pipeline(silver_results or {})

    bronze_out = run_bronze()
    silver_out = run_silver(bronze_out)
    _ = run_gold(silver_out)

dag = spaceparts_etl_orchestrator()
