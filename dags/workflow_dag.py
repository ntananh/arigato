import json
import os
import re
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.models import Variable, BaseOperator
from airflow.exceptions import AirflowException

from tasks.clean import DataCleaningTask
from tasks.collect import DataCollectionTask
from tasks.job_matching import JobMatchingTask
from tasks.load_preference import LoadPreferenceTask
from tasks.notify import NotificationTask
from tasks.persistent import JobDatabaseOperator
from tasks.report import HtmlReportTask
from tasks.verify import VerificationTask

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 4, 8),
}

def get_variable_with_fallback(name: str, default: Any) -> Any:
    try:
        value = Variable.get(name, default_var=json.dumps(default))
        return json.loads(value)
    except Exception as e:
        logger.error(f"Failed to parse variable {name}: {str(e)}")
        raise AirflowException(f"Configuration error: Invalid {name} variable")

def validate_sources(sources: List[str]) -> None:
    if not isinstance(sources, list) or len(sources) == 0:
        raise AirflowException("data_sources must be a non-empty list")
    if any(not source or not isinstance(source, str) for source in sources):
        raise AirflowException("All sources must be non-empty strings")

def create_collection_tasks(dag: DAG) -> Dict[str, BaseOperator]:
    sources = get_variable_with_fallback('data_sources', ["linkedin", "upwork"])
    job_types = get_variable_with_fallback('job_types', ["software engineer", "data scientist"])
    keywords = get_variable_with_fallback('keywords', ["python", "machine learning"])
    locations = get_variable_with_fallback('locations', ["remote", "finland"])
    max_results = int(Variable.get('max_results', '100'))

    validate_sources(sources)

    tasks = {}
    for source in sources:
        safe_source = re.sub(r'[^a-zA-Z0-9_]', '_', str(source).lower()).strip('_')
        if not safe_source:
            logger.warning(f"Skipping invalid source name: {source}")
            continue

        task_id = f'collect_{safe_source}_data'
        if task_id in tasks:
            raise AirflowException(f"Duplicate task ID generated: {task_id}")

        tasks[source] = DataCollectionTask(
            task_id=task_id,
            source=source,
            job_types=job_types,
            keywords=keywords,
            locations=locations,
            max_results=max_results,
            dag=dag
        )
        logger.info(f"Created collection task for {source}")

    return tasks

with DAG(
    'job_pipeline',
    default_args=default_args,
    description='Job posting collection, processing and matching pipeline',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['jobs', 'data_pipeline'],
) as dag:

    load_preferences = LoadPreferenceTask(
        task_id='load_preferences',
        dag=dag
    )

    verify_input = VerificationTask(
        task_id='verify_input',
        dag=dag
    )

    collection_tasks = create_collection_tasks(dag)

    clean_data = DataCleaningTask(
        task_id='clean_data',
        csv_output_dir='data/clean',
        dag=dag
    )

    match_jobs = JobMatchingTask(
        task_id='match_jobs',
        preferences_task_id='load_preferences',
        dag=dag
    )

    generate_report = HtmlReportTask(
        task_id='generate_report',
        output_dir='data/reports',
        dag=dag
    )

    save_jobs_task = JobDatabaseOperator(
        task_id='save_matched_jobs',
        dag=dag
    )

    email = Variable.get('notification_email', 'anh.4.nguyen@tuni.fi')
    min_score = float(Variable.get('min_match_score', '0.5'))
    max_notify_jobs = int(Variable.get('max_notification_jobs', '10'))

    send_notifications = NotificationTask(
        task_id='send_notifications',
        email=email,
        min_score=min_score,
        max_jobs=max_notify_jobs,
        dag=dag
    )

    verify_input >> list(collection_tasks.values())
    for task in collection_tasks.values():
        task >> clean_data

    load_preferences >> match_jobs
    clean_data >> match_jobs
    match_jobs >> [generate_report, send_notifications, save_jobs_task]

    logger.info("DAG structure created successfully")