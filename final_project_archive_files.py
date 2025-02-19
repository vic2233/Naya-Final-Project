from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

default_args = {
    'owner': 'Victoria',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 15),
    'email': ['vic2233@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'archive_parquet_files',
    default_args=default_args,
    description='Daily job to move files from daily bucket to historical bucket, divided to partition by day',
    schedule_interval='@daily'
) as dag:

    # Task 1: Archive purquet files to historical bucket
    archive_files_task = SSHOperator(
        task_id="archive_files",
        ssh_conn_id="dev_env_ssh", 
        command="python3 /home/developer/projects/FinalProject/Archive/5.ArchiveFiles.py"
    )

    # Task 2: Delete real time- daily purquet files
    delete_rtm_files_task = SSHOperator(
        task_id="delete_rtm_files",
        ssh_conn_id="dev_env_ssh", 
        command="python3 /home/developer/projects/FinalProject/Archive/6.DeleteFiles.py"
    )


# Task dependencies
archive_files_task >> delete_rtm_files_task



