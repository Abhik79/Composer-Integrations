from airflow import DAG
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime, timedelta
import json
import base64
import logging

# Define your GCP project ID, subscription, and connection ID
PROJECT_ID = "coastal-throne-433510-a5"
SUBSCRIPTION_NAME = "Airflow-Bigquery-SP-Trigger-sub"
GCP_CONNECTION_ID = "google_cloud_default"

# Define stored procedure names
SP_TABLE_SNAPSHOTS = "coastal-throne-433510-a5.table_snapshots.sp_table_snapshots"
SP_DYNAMIC_PIVOT = "coastal-throne-433510-a5.bq_dev_new.sp_dynamic_pivot"

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2023, 10, 20),
}

# Define the DAG
with DAG(
    "pubsub_triggered_sp_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # Task 1: Wait for a message from the Pub/Sub subscription
    wait_for_pubsub_message = PubSubPullSensor(
        task_id="wait_for_pubsub_message",
        project_id=PROJECT_ID,
        subscription=SUBSCRIPTION_NAME,
        ack_messages=True,
        timeout=60,  # Set appropriate timeout for your use case
    )

    # Task 2: Parse the Pub/Sub message and determine which stored procedure to run
    def process_pubsub_message(**kwargs):
        messages = kwargs['ti'].xcom_pull(task_ids='wait_for_pubsub_message')
        
        for message in messages:
            try:
                raw_data = message['message']['data']
                logging.info(f"Raw data received: {raw_data}")
                decoded_data = base64.b64decode(raw_data).decode("utf-8")
                logging.info(f"Decoded data: {decoded_data}")
                message_data = json.loads(decoded_data)
                logging.info(f"Parsed message data: {message_data}")

                sp_name = message_data.get("sp_name")
                if sp_name == "sp_table_snapshots":
                    kwargs['ti'].xcom_push(key='sp_to_run', value='sp_table_snapshots')
                    logging.info("Triggering sp_table_snapshots")
                    return
                elif sp_name == "sp_dynamic_pivot":
                    kwargs['ti'].xcom_push(key='sp_to_run', value='sp_dynamic_pivot')
                    logging.info("Triggering sp_dynamic_pivot")
                    return
                else:
                    logging.warning("Unknown sp_name received")
                    raise ValueError("Unknown stored procedure name")
            except Exception as e:
                logging.error(f"Error processing message: {e}")
                raise

    determine_sp_task = PythonOperator(
        task_id="determine_sp_task",
        python_callable=process_pubsub_message,
        provide_context=True,
    )

    # Task 3: Branching task to decide which stored procedure to run
    def branch_task(**kwargs):
        sp_to_run = kwargs['ti'].xcom_pull(task_ids='determine_sp_task', key='sp_to_run')
        if sp_to_run == 'sp_table_snapshots':
            return 'run_sp_table_snapshots'
        elif sp_to_run == 'sp_dynamic_pivot':
            return 'run_sp_dynamic_pivot'
        else:
            raise ValueError("No valid stored procedure found to run.")

    branching_task = BranchPythonOperator(
        task_id='branching_task',
        python_callable=branch_task,
        provide_context=True,
    )

    # Task 4: Run the 'sp_table_snapshots' stored procedure
    run_sp_table_snapshots = BigQueryInsertJobOperator(
        task_id="run_sp_table_snapshots",
        configuration={
            "query": {
                "query": f"CALL `{SP_TABLE_SNAPSHOTS}`()",
                "useLegacySql": False,
            }
        },
        gcp_conn_id=GCP_CONNECTION_ID,
    )

    # Task 5: Run the 'sp_dynamic_pivot' stored procedure
    run_sp_dynamic_pivot = BigQueryInsertJobOperator(
        task_id="run_sp_dynamic_pivot",
        configuration={
            "query": {
                "query": f"CALL `{SP_DYNAMIC_PIVOT}`()",
                "useLegacySql": False,
            }
        },
        gcp_conn_id=GCP_CONNECTION_ID,
    )

    # Define the task dependencies
    wait_for_pubsub_message >> determine_sp_task
    determine_sp_task >> branching_task
    branching_task >> [run_sp_table_snapshots, run_sp_dynamic_pivot]
