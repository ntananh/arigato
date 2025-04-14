from datetime import datetime
import csv
import logging
import os
import json
from typing import Dict, Any, Optional, List

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

logger = logging.getLogger(__name__)

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

    def _parse_salary(self, salary_str: str) -> Dict[str, Any]:
        try:
            if salary_str and isinstance(salary_str, str) and \
               (salary_str.startswith('{') or salary_str.startswith("'")):
                salary_str = salary_str.replace("'", '"')
                return json.loads(salary_str)

            return {
                'min': None,
                'max': None,
                'currency': None,
                'period': None
            }
        except (json.JSONDecodeError, TypeError):
            return {
                'min': None,
                'max': None,
                'currency': None,
                'period': None
            }

    def _parse_skills(self, skills_str: str) -> List[str]:
        if not skills_str:
            return []

        return [skill.strip() for skill in skills_str.split(',') if skill.strip()]

    def _insert_job_listing(self, job_data: Dict[str, Any]) -> Optional[int]:
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
        airflow_home = os.environ.get('AIRFLOW_HOME', '')
        file_path = os.path.join(airflow_home, 'data/top_matched_jobs.csv')

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                csv_reader = csv.DictReader(f)
                jobs = list(csv_reader)
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
            return []
        except Exception as e:
            logger.error(f"Error reading CSV: {e}")
            return []

        inserted_job_ids = []

        for job in jobs:
            try:
                # Parse salary
                salary = self._parse_salary(job.get('salary', '{}'))

                # Parse skills
                skills = self._parse_skills(job.get('skills', ''))

                job_data = {
                    'external_id': str(job.get('id', '')),
                    'title': job.get('title', ''),
                    'description': job.get('description', ''),
                    'source': job.get('source', 'upwork'),
                    'url': job.get('url', ''),
                    'posted_date': job.get('posted_date') or datetime.now().date(),
                    'job_type': job.get('job_type', ''),
                    'employment_type': job.get('employment_type', ''),
                    'remote': job.get('remote', 'False').lower() in ['true', '1', 'yes'],
                    'location': job.get('location', ''),

                    # Salary parsing
                    'salary_min': salary.get('min'),
                    'salary_max': salary.get('max'),
                    'salary_currency': salary.get('currency'),
                    'salary_period': salary.get('period'),

                    # Scoring
                    'match_score': float(job.get('match_score', 0)),
                    'skill_score': float(job.get('skill_score', 0)),
                    'location_score': float(job.get('location_score', 0)),
                    'salary_score': float(job.get('salary_score', 0)),
                    'company_score': float(job.get('company_score', 0)),
                    'description_score': float(job.get('description_score', 0)),

                    # City and country parsing
                    'city': job.get('city', ''),
                    'country': job.get('country', ''),

                    # Skills
                    'skills': skills
                }

                job_id = self._insert_job_listing(job_data)
                inserted_job_ids.append(job_id)

            except Exception as e:
                logger.error(f"Error inserting job {job.get('id')}: {e}")

        logger.info(f"Inserted {len(inserted_job_ids)} jobs")

        context['ti'].xcom_push(key='inserted_job_ids', value=inserted_job_ids)

        return inserted_job_ids