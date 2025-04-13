from typing import Dict, Any, Optional

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.decorators import apply_defaults

class JobDatabaseOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        conn_id='postgres_default',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id

    def insert_job_listing(self, job_data: Dict[str, Any]) -> Optional[int]:
        pg_hook = PostgresHook(postgres_conn_id=self.conn_id)

        job_listing_sql = """
        INSERT INTO job_listings (
            external_id, title, description, source, url,
            posted_date, job_type, employment_type, remote,
            location, city, country,
            salary_min, salary_max, salary_currency, salary_period,
            match_score, skill_score, location_score,
            salary_score, company_score, description_score
        ) VALUES (
            %(external_id)s, %(title)s, %(description)s, %(source)s, %(url)s,
            %(posted_date)s, %(job_type)s, %(employment_type)s, %(remote)s,
            %(location)s, %(city)s, %(country)s,
            %(salary_min)s, %(salary_max)s, %(salary_currency)s, %(salary_period)s,
            %(match_score)s, %(skill_score)s, %(location_score)s,
            %(salary_score)s, %(company_score)s, %(description_score)s
        ) RETURNING id
        """

        job_id = pg_hook.get_first(job_listing_sql, parameters=job_data)[0]

        if job_data.get('skills'):
            self._insert_job_skills(pg_hook, job_id, job_data['skills'])

        if job_data['source'] == 'upwork':
            self._insert_upwork_details(pg_hook, job_id, job_data)

        return job_id

    def _insert_job_skills(self, pg_hook, job_id, skills):
        skill_insert_sql = """
        WITH inserted_skills AS (
            INSERT INTO skills (name)
            VALUES unnest(%(skills)s)
            ON CONFLICT (name) DO NOTHING
            RETURNING id
        )
        INSERT INTO job_skills (job_id, skill_id)
        SELECT %(job_id)s, id
        FROM (
            SELECT id FROM inserted_skills
            UNION
            SELECT id FROM skills WHERE name = ANY(%(skills)s)
        ) unique_skills
        """
        pg_hook.run(skill_insert_sql, parameters={
            'job_id': job_id,
            'skills': skills
        })

    def _insert_upwork_details(self, pg_hook, job_id, job_data):
        upwork_details_sql = """
        INSERT INTO upwork_client_details (
            job_id, client_total_jobs_posted, client_rating,
            client_country, engagement_duration,
            weekly_hours, project_type
        ) VALUES (
            %(job_id)s, %(client_total_jobs_posted)s, %(client_rating)s,
            %(client_country)s, %(engagement_duration)s,
            %(weekly_hours)s, %(project_type)s
        )
        """
        pg_hook.run(upwork_details_sql, parameters={
            'job_id': job_id,
            **job_data.get('upwork_details', {})
        })

    def execute(self, context):
        pass