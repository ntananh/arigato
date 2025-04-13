from datetime import datetime, timedelta
from airflow import DAG
import json
import os
import re
import csv
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from hashlib import md5
from difflib import SequenceMatcher
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

logger = logging.getLogger(__name__)

class DataCleaningTask(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        csv_output_dir: str = None,
        json_input_dir: str = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        airflow_home = os.environ.get('AIRFLOW_HOME', '')
        self.csv_output_dir = csv_output_dir or os.path.join(
            airflow_home, 'data/clean')
        self.json_input_dir = json_input_dir or os.path.join(
            airflow_home, 'data/json')
        os.makedirs(self.csv_output_dir, exist_ok=True)
        os.makedirs(self.json_input_dir, exist_ok=True)

        self.tech_skills = [
            'python', 'javascript', 'java', 'c++', 'c#', 'go', 'rust',
            'react', 'angular', 'vue', 'node', 'django', 'flask', 'spring',
            'sql', 'mysql', 'postgresql', 'mongodb', 'aws', 'azure', 'gcp',
            'docker', 'kubernetes', 'terraform', 'ansible', 'jenkins',
            'machine learning', 'ai', 'data science', 'data engineering'
        ]

        self.salary_patterns = [
            r'(?P<currency>\$|€|£)?\s*(?P<min>[\d,]+)\s*[-–to]+\s*(?P<currency2>\$|€|£)?\s*(?P<max>[\d,]+)\s*(?P<period>per year|per month|per hour|yearly|monthly|hourly)?',
            r'(?P<currency>\$|€|£)?\s*(?P<min>[\d,]+k)\s*[-–to]+\s*(?P<currency2>\$|€|£)?\s*(?P<max>[\d,]+k)\s*(?P<period>per year|per month|per hour|yearly|monthly|hourly)?',
            r'(?P<min>[\d,]+)\s*[-–to]+\s*(?P<max>[\d,]+)\s*(?P<currency>USD|EUR|GBP)',
        ]

    def execute(self, context):
        all_jobs = self._collect_jobs_from_files()
        if not all_jobs:
            logger.warning("No jobs found in input files")
            return []

        cleaned_jobs = self.clean(all_jobs)
        context['ti'].xcom_push(key='cleaned_jobs', value=cleaned_jobs)
        return cleaned_jobs

    def clean(self, jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        cleaned_jobs = [job for job in jobs if self._is_valid_job(job)]
        deduplicated_jobs = self._deduplicate_jobs(cleaned_jobs)

        processed_jobs = []
        for job in deduplicated_jobs:
            try:
                processed_job = self._clean_job(job)
                logger.debug(f"Processed job: {processed_job}")
                if processed_job:
                    processed_jobs.append(processed_job)
            except Exception as e:
                logger.error(f"Error processing job: {str(e)}")

        self._save_to_csv(processed_jobs, 'cleaned_jobs.csv')
        logger.info(f"Processed {len(processed_jobs)}/{len(jobs)} jobs")
        return processed_jobs

    def _is_valid_job(self, job: Dict[str, Any]) -> bool:
        if not job or not isinstance(job, dict):
            return False

        # Minimum required fields
        required_fields = ['title']

        # Check for at least some meaningful content
        for field in required_fields:
            value = job.get(field, '').strip()
            if not value or len(value) < 10:  # Require at least some meaningful text
                return False

        # Optional additional checks
        if 'source' in job:
            valid_sources = ['linkedin', 'upwork', 'indeed']
            if job['source'].lower() not in valid_sources:
                return False

        # Check for any suspicious patterns or known spam indicators
        suspicious_patterns = [
            'test job', 'sample job', 'fake job',
            'placeholder', '[job description]'
        ]

        title = job.get('title', '').lower()
        description = job.get('description', '').lower()

        for pattern in suspicious_patterns:
            if pattern in title or pattern in description:
                return False

        return True

    def _deduplicate_jobs(self, jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        def generate_job_signature(job: Dict[str, Any]) -> str:
            key_fields = ['title', 'description', 'url']
            signature_parts = [str(job.get(field, '')).lower().strip()
                               for field in key_fields]
            return md5('|'.join(signature_parts).encode()).hexdigest()

        def job_similarity(job1: Dict[str, Any], job2: Dict[str, Any]) -> float:
            title_sim = SequenceMatcher(None,
                                        str(job1.get('title', '')).lower(),
                                        str(job2.get('title', '')).lower()
                                        ).ratio()
            desc_sim = SequenceMatcher(None,
                                       str(job1.get('description', '')).lower(),
                                       str(job2.get('description', '')).lower()
                                       ).ratio()
            return (title_sim + desc_sim) / 2

        unique_jobs = {}
        for job in jobs:
            signature = generate_job_signature(job)

            is_duplicate = False
            for existing_sig, existing_job in list(unique_jobs.items()):
                if job_similarity(job, existing_job) > 0.9:
                    is_duplicate = True
                    break

            if not is_duplicate:
                unique_jobs[signature] = job

            logger.debug(f"Unique job: {unique_jobs.__len__}")
        return list(unique_jobs.values())

    def _clean_job(self, job: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            source = self._determine_source(job)
            common_fields = self._extract_common_fields(job, source)
            source_fields = self._extract_source_specific_fields(job, source)
            return {**common_fields, **source_fields}
        except Exception as e:
            logger.error(f"Error cleaning job: {str(e)}")
            return None

    def _collect_jobs_from_files(self) -> List[Dict[str, Any]]:
        source_files = {
            'linkedin': 'linkedin_jobs.json',
            'upwork': 'upwork_jobs.json'
        }
        return self._load_jobs_from_files(source_files)

    def _load_jobs_from_files(self, source_files: Dict[str, str]) -> List[Dict[str, Any]]:
        all_jobs = []
        for source, filename in source_files.items():
            filepath = os.path.join(self.json_input_dir, filename)
            try:
                with open(filepath, 'r') as f:
                    jobs = json.load(f)
                    if jobs and isinstance(jobs, list):
                        all_jobs.extend(jobs)
                        logger.info(f"Loaded {len(jobs)} jobs from {filepath}")
            except Exception as e:
                logger.error(f"Error loading {filepath}: {str(e)}")
        return all_jobs

    def _determine_source(self, job: Dict[str, Any]) -> str:
        job_str = str(job).lower()
        if 'linkedin' in str(job.get('id', '')).lower() or 'linkedin' in job_str:
            return 'linkedin'
        if 'client' in job or 'upwork.com' in str(job.get('url', '')):
            return 'upwork'
        return 'unknown'

    def _extract_common_fields(self, job: Dict[str, Any], source: str) -> Dict[str, Any]:
        return {
            'id': str(job.get('id', '')),
            'title': self._clean_text(job.get('title', '')),
            'company': self._clean_text(self._extract_company(job)),
            'location': self._clean_text(self._extract_location(job)),
            'description': self._clean_text(self._extract_description(job)),
            'url': job.get('url', ''),
            'remote': self._is_remote(job),
            'source': source,
            'posted_date': self._parse_date(job.get('date_posted')),
            'skills': self._extract_skills(job),
            'salary': self._extract_salary(job, source)
        }

    def _extract_source_specific_fields(self, job: Dict[str, Any], source: str) -> Dict[str, Any]:
        if source == 'linkedin':
            return self._process_linkedin_fields(job)
        elif source == 'upwork':
            return self._process_upwork_fields(job)
        return {}

    def _clean_text(self, text: str) -> str:
        if not text:
            return ''
        return ' '.join(''.join(c for c in str(text) if c.isprintable()).split()).strip()

    def _extract_company(self, job: Dict[str, Any]) -> str:
        if 'organization' in job:
            return str(job['organization'])
        if 'client' in job and isinstance(job['client'], dict):
            return str(job['client'].get('name', ''))
        return str(job.get('company', ''))

    def _extract_location(self, job: Dict[str, Any]) -> str:
        if 'locations_derived' in job and job['locations_derived']:
            return ', '.join(job['locations_derived'][:3])
        if 'client_country' in job:
            return job['client_country']
        return str(job.get('location', 'Remote'))

    def _extract_description(self, job: Dict[str, Any]) -> str:
        return str(job.get('description_text', job.get('description', '')))

    def _is_remote(self, job: Dict[str, Any]) -> bool:
        if 'remote_derived' in job:
            return bool(job['remote_derived'])

        text = f"{self._extract_location(job)} {self._extract_description(job)}".lower(
        )
        return any(term in text for term in ['remote', 'work from home', 'wfh', 'virtual'])

    def _parse_date(self, date_str: Optional[str]) -> Optional[str]:
        if not date_str:
            return None

        for fmt in ('%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
            try:
                dt = datetime.strptime(date_str.split('+')[0], fmt)
                return dt.strftime('%Y-%m-%d')
            except ValueError:
                continue
        return None

    def _extract_skills(self, job: Dict[str, Any]) -> List[str]:
        skills = set()

        for field in ['skills_additional', 'skills_occupation', 'skills']:
            if field in job and job[field]:
                skills.update(str(s).lower() for s in (
                    job[field] if isinstance(job[field], list)
                    else [s.strip() for s in job[field].split(',')]
                ))

        desc = self._extract_description(job).lower()
        skills.update(
            skill for skill in self.tech_skills
            if re.search(r'\b' + re.escape(skill) + r'\b', desc)
        )

        return sorted(skills)

    def _extract_salary(self, job: Dict[str, Any], source: str) -> Optional[Dict[str, Any]]:
        if source == 'linkedin':
            return self._extract_linkedin_salary(job)
        elif source == 'upwork':
            return self._extract_upwork_salary(job)
        return None

    def _extract_linkedin_salary(self, job: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if salary := self._parse_salary_string(job.get('salary_raw')):
            return salary
        return self._parse_salary_string(self._extract_description(job))

    def _extract_upwork_salary(self, job: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if 'project_budget_hourly_min' in job and job['project_budget_hourly_min']:
            return {
                'min': float(job['project_budget_hourly_min']),
                'max': float(job.get('project_budget_hourly_max', job['project_budget_hourly_min'])),
                'currency': job.get('project_budget_currency', 'USD'),
                'period': 'hour'
            }

        if 'project_budget_total' in job and job['project_budget_total']:
            return {
                'min': float(job['project_budget_total']),
                'max': float(job['project_budget_total']),
                'currency': job.get('project_budget_currency', 'USD'),
                'period': 'fixed'
            }

        return None

    def _parse_salary_string(self, text: Optional[str]) -> Optional[Dict[str, Any]]:
        if not text or not isinstance(text, str):
            return None

        for pattern in self.salary_patterns:
            if match := re.search(pattern, text, re.IGNORECASE):
                groups = match.groupdict()
                currency = groups.get('currency') or groups.get(
                    'currency2') or 'USD'

                min_salary = groups['min'].replace('k', '000').replace(',', '')
                max_salary = groups['max'].replace('k', '000').replace(',', '')

                return {
                    'min': float(min_salary),
                    'max': float(max_salary),
                    'currency': currency,
                    'period': (groups.get('period') or 'year').lower().replace('per ', '')
                }
        return None

    def _process_linkedin_fields(self, job: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'employment_type': ', '.join(job['employment_type'])
            if isinstance(job.get('employment_type'), list)
            else job.get('employment_type'),
            'company_size': job.get('linkedin_org_size'),
            'company_industry': job.get('linkedin_org_industry'),
            'company_description': self._clean_text(job.get('linkedin_org_description')),
            'city': job['cities_derived'][0] if job.get('cities_derived') else None
        }

    def _process_upwork_fields(self, job: Dict[str, Any]) -> Dict[str, Any]:
        fields = {
            'job_type': job.get('project_type', 'Contract'),
            'client_rating': job.get('client_score'),
            'client_jobs_posted': job.get('client_total_assignments'),
            'engagement_duration': job.get('engagement_duration', {}).get('label'),
            'weekly_hours': job.get('weekly_hours')
        }

        if 'category' in job:
            fields.update({
                'category': job['category'],
                'category_group': job.get('category_group')
            })

        return fields

    def _save_to_csv(self, jobs: List[Dict[str, Any]], filename: str) -> None:
        if not jobs:
            logger.warning("No jobs to save to CSV")
            return

        filepath = os.path.join(self.csv_output_dir, filename)

        try:
            fieldnames = sorted({k for job in jobs for k in job.keys()})

            flat_jobs = [
                {k: str(v) if isinstance(v, (list, dict)) else v
                 for k, v in job.items()}
                for job in jobs
            ]

            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(flat_jobs)

            logger.info(f"Saved {len(jobs)} jobs to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save CSV: {str(e)}", exc_info=True)
