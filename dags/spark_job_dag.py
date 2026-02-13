from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
}

dag = DAG(
    dag_id='pyspark_sample_dag',
    default_args=default_args,
    schedule_interval=None,  # Run manually
    catchup=False,
)

# Bash command to submit PySpark job
pyspark_task = BashOperator(
    task_id='run_pyspark_job',
    bash_command="""
    spark-submit --master spark://spark-master:7077 /opt/airflow/dags/pyspark_job.py
    """,
    dag=dag,
)
