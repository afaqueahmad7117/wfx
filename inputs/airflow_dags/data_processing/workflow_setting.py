from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta
import logging

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email": ["data-alerts@company.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def failure_callback(context):
    """Send notification on task failure"""
    message = (
        f"Task Failed!\n"
        f"DAG: {context['task_instance'].dag_id}\n"
        f"Task: {context['task_instance'].task_id}\n"
        f"Execution Time: {context['task_instance'].start_date}\n"
        f"Error: {context['exception']}"
    )
    logger.error(message)


def success_callback(context):
    logger.info(f"Task {context['task_instance'].task_id} completed successfully")


default_args["on_failure_callback"] = failure_callback
default_args["on_success_callback"] = success_callback

dag = DAG(
    "sample_etl_pipeline",
    default_args=default_args,
    description="A sample ETL pipeline with complex dependencies",
    schedule_interval="0 1 * * *",  # Run at 1 AM daily
    catchup=False,
)


def process_data(**context):
    """Sample data processing function"""
    logger.info("Processing data...")
    return "Data processed successfully"


validate_input_data = BashOperator(
    task_id="validate_input_data",
    bash_command='echo "Checking input data..." && sleep 5',
    dag=dag,
)

process_data = PythonOperator(
    task_id="process_data", python_callable=process_data, provide_context=True, dag=dag
)

export_processed_data = BashOperator(
    task_id="export_processed_data",
    bash_command='echo "Exporting processed data..." && sleep 5',
    dag=dag,
)

generate_report = BashOperator(
    task_id="generate_report",
    bash_command='echo "Generating report..." && sleep 5',
    dag=dag,
)


def trigger_condition(context, dag_run_obj):
    return dag_run_obj


trigger_downstream_dag = TriggerDagRunOperator(
    task_id="trigger_downstream_dag",
    trigger_dag_id="downstream_analytics_dag",
    python_callable=trigger_condition,
    dag=dag,
)

validate_input_data >> process_data >> [export_processed_data, generate_report] >> trigger_downstream_dag

