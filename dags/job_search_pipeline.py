from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from plugins.job_search_operators.job_source_operator import JobSourceOperator
from plugins.job_search_operators.job_matching_operator import JobMatchingOperator
from plugins.job_search_operators.notification_operator import NotificationOperator

def load_job_preferences():
    print("Loading job preferences")
    pass

with DAG(
    'job_search_pipeline',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2023, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval='@daily',
    catchup=False
) as dag:

    load_preferences = PythonOperator(
        task_id='load_job_preferences',
        python_callable=load_job_preferences
    )

    fetch_linkedin_jobs = JobSourceOperator(
        task_id='fetch_linkedin_jobs',
        source='linkedin'
    )

    fetch_upwork_jobs = JobSourceOperator(
        task_id='fetch_upwork_jobs',
        source='upwork'
    )

    match_jobs = JobMatchingOperator(
        task_id='match_jobs',
        sources=[
            '{{ ti.xcom_pull(task_ids="fetch_linkedin_jobs") }}',
            '{{ ti.xcom_pull(task_ids="fetch_upwork_jobs") }}'
        ]
    )

    send_notifications = NotificationOperator(
        task_id='send_job_notifications',
        matched_jobs='{{ ti.xcom_pull(task_ids="match_jobs") }}'
    )

    # Define task dependencies
    load_preferences >> [fetch_linkedin_jobs, fetch_upwork_jobs]
    [fetch_upwork_jobs, fetch_linkedin_jobs] >> match_jobs
    match_jobs >> send_notifications


