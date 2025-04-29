from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments
default_args = {
    "owner": "[USER_NAME]",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 23),
    "email": ["[EMAIL_ADDRESS]"],
    "email_on_failure": False,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

# DAG definition
with DAG(
    "youtube_analytics_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["YouTube", "Analytics", "Spark"],
) as dag:
    run_scraper = BashOperator(
        task_id="run_youtube_scraper",
        bash_command="""
        source /path/to/virtualenv/bin/activate && \
        python3 /path/to/youtube_scraper.py
        """,
    )

    run_transform = BashOperator(
        task_id="run_spark_transform",
        bash_command="""
        source /path/to/virtualenv/bin/activate && \
        python3 /path/to/spark_transform.py
        """,
    )

    run_scraper >> run_transform