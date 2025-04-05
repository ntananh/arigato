import os
import json
from typing import List, Dict, Any, Optional

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook

class JobSourceOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        source: str,
        job_types: List[str],
        keywords: List[str],
        max_results: int = 50,
        locations: Optional[List[str]] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.source = source.lower()
        self.job_types = job_types
        self.locations = locations or ['remote']
        self.keywords = keywords
        self.max_results = max_results

        # Load headers from config
        headers_path = os.path.join(os.environ.get('AIRFLOW_HOME', ''), 'include', 'config', 'headers.json')
        with open(headers_path, 'r') as headers_file:
            self.headers_config = json.load(headers_file)

    def execute(self, context: Dict[str, Any]) -> List[Dict[str, Any]]:
        self.log.info(f"Collecting jobs from {self.source}...")

        if self.source == 'linkedin':
            hook = DynamicRapidApiHook.create("linkedin")
        elif self.source == 'upwork':
            hook =  DynamicRapidApiHook.create("upwork")
        else:
            raise ValueError(f"Unsupported source: {self.source}")

        jobs = []
        for job_type in self.job_types:
            for location in self.locations:
                try:
                    self.log.info(f"Searching for {job_type} jobs in {location}")
                    results = hook.search_jobs(
                        job_type=job_type,
                        location=location,
                        keywords=self.keywords,
                        max_results=self.max_results
                    )
                    jobs.extend(results)
                    self.log.info(f"Found {len(results)} jobs for {job_type} in {location}")
                except Exception as e:
                    self.log.error(f"Error collecting {job_type} jobs in {location}: {str(e)}")

        self._store_jobs(jobs)

        self.log.info(f"Collected and stored {len(jobs)} jobs from {self.source}")
        return jobs

    def _store_jobs(self, jobs: List[Dict[str, Any]]) -> None:
        if not jobs:
            self.log.warning("No jobs to store")
            return

        pg_hook = PostgresHook(postgres_conn_id='postgres_default')

        for job in jobs:
            try:
                pg_hook.run(
                    """
                    INSERT INTO raw_job_postings
                    (source, source_id, job_title, company_name, location, raw_data)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source, source_id) DO UPDATE
                    SET job_title = EXCLUDED.job_title,
                        company_name = EXCLUDED.company_name,
                        location = EXCLUDED.location,
                        raw_data = EXCLUDED.raw_data,
                        collected_at = CURRENT_TIMESTAMP
                    """,
                    parameters=(
                        self.source,
                        job.get('id', ''),
                        job.get('title', ''),
                        job.get('company', ''),
                        job.get('location', ''),
                        json.dumps(job)
                    )
                )
            except Exception as e:
                self.log.error(f"Error storing job {job.get('id', '')}: {str(e)}")