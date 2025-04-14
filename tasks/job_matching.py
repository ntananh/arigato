import logging
import os
import sys
import yaml
import csv
from typing import Dict, Any, List

from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults

logger = logging.getLogger(__name__)

class JobMatchingTask(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        cleaned_jobs_path: str = None,
        preferences_task_id: str = "load_preferences",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.preferences_task_id = preferences_task_id
        airflow_home = os.environ.get('AIRFLOW_HOME', '')
        self.cleaned_jobs_path = cleaned_jobs_path or os.path.join(airflow_home, 'data/clean/cleaned_jobs.csv')

    def execute(self, context):
        ti = context['ti']

        cleaned_jobs = self._load_cleaned_jobs()
        if not cleaned_jobs:
            logger.error(f"No cleaned jobs found at {self.cleaned_jobs_path}")
            raise ValueError(f"Could not load cleaned jobs from {self.cleaned_jobs_path}")

        user_profile = ti.xcom_pull(task_ids=self.preferences_task_id)
        if not user_profile:
            logger.error(f"Failed to get preferences from task {self.preferences_task_id}")
            raise ValueError(f"Could not retrieve user preferences from {self.preferences_task_id}")

        logger.info(f"Matching {len(cleaned_jobs)} jobs using preferences from {self.preferences_task_id}")

        matched_jobs = self.match(cleaned_jobs, user_profile)

        for job in matched_jobs[:10]:
            logger.info(f"\tMatched Job: {job.get('title', 'Untitled')} - Score: {float.__round__(job.get('match_score', 0), 2)} - URL: {job.get('url')}")

        ti.xcom_push(key='matched_jobs', value=matched_jobs)

        return matched_jobs

    def _load_cleaned_jobs(self) -> List[Dict[str, Any]]:
        """Load cleaned jobs from CSV file"""
        if not os.path.exists(self.cleaned_jobs_path):
            logger.warning(f"Cleaned jobs file not found: {self.cleaned_jobs_path}")
            return []

        jobs = []
        try:
            with open(self.cleaned_jobs_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    processed_row = {}
                    for k, v in row.items():
                        if v.startswith('[') or v.startswith('{'):
                            try:
                                processed_row[k] = eval(v)
                            except:
                                processed_row[k] = v
                        else:
                            processed_row[k] = v
                    jobs.append(processed_row)

            logger.info(f"Loaded {len(jobs)} jobs from {self.cleaned_jobs_path}")
            return jobs

        except Exception as e:
            logger.error(f"Error loading cleaned jobs: {str(e)}")
            return []


    def match(self, jobs: List[Dict[str, Any]], user_profile: Dict[str, Any]) -> List[Dict[str, Any]]:
        if not jobs:
            logger.warning("No jobs to match")
            return []

        logger.info(f"Matching {len(jobs)} jobs with user profile")

        matcher = self._get_matcher_model(user_profile)

        matched_jobs = []
        for job in jobs:
            try:
                match_scores = matcher.match_job(job)

                matched_job = job.copy()
                matched_job.update(match_scores)

                matched_jobs.append(matched_job)

            except Exception as e:
                job_id = job.get('id', 'unknown')
                logger.error(f"Error matching job {job_id}: {str(e)}")

        matched_jobs.sort(key=lambda x: x.get('match_score', 0), reverse=True)


        self._save_to_csv(matched_jobs[:50], 'top_matched_jobs.csv')

        logger.info(f"Matched {len(matched_jobs)} jobs")
        return matched_jobs

    def _get_matcher_model(self, user_profile: Dict[str, Any]):
        try:
            plugins_path = os.path.join(os.environ.get('AIRFLOW_HOME', ''), 'plugins')
            if plugins_path not in sys.path:
                sys.path.insert(0, plugins_path)

            from ml_models.job_matcher_model import JobMatcherModel

            preferences_path = Variable.get('preferences_path', 'include/config/job_preferences.yaml')

            if preferences_path and os.path.exists(preferences_path):
                matcher = JobMatcherModel(preferences_path)
            else:
                temp_dir = os.path.join(os.environ.get('AIRFLOW_HOME', ''), 'data', 'temp')
                os.makedirs(temp_dir, exist_ok=True)
                temp_path = os.path.join(temp_dir, 'temp_preferences.yaml')

                with open(temp_path, 'w') as f:
                    yaml.dump(user_profile, f)

                matcher = JobMatcherModel(temp_path)

            return matcher

        except ImportError:
            logger.warning("Could not import JobMatcherModel, using simplified version")
            return self.SimpleJobMatcher(user_profile)

    def _save_to_csv(self, jobs: List[Dict[str, Any]], filename: str) -> None:
        if not jobs:
            logger.warning(f"No jobs to save to {filename}")
            return

        data_dir = os.path.join(os.environ.get('AIRFLOW_HOME', ''), 'data')
        os.makedirs(data_dir, exist_ok=True)
        csv_path = os.path.join(data_dir, filename)

        try:
            import csv

            fieldnames = set()
            for job in jobs:
                fieldnames.update(job.keys())
            fieldnames = sorted(list(fieldnames))

            score_fields = ['match_score', 'skill_score', 'location_score', 'salary_score',
                            'company_score', 'description_score']
            for field in score_fields:
                if field not in fieldnames:
                    fieldnames.append(field)

            with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

                for job in jobs:
                    if 'skills' in job and isinstance(job['skills'], list):
                        job['skills'] = ','.join(job['skills'])

                    writer.writerow(job)

            logger.info(f"Saved {len(jobs)} jobs to CSV: {csv_path}")

        except Exception as e:
            logger.error(f"Error saving jobs to CSV: {str(e)}")

    class SimpleJobMatcher:
        def __init__(self, user_profile: Dict[str, Any]):
            self.user_profile = user_profile

            self.must_have_skills = user_profile.get('skills', {}).get('must_have', [])
            self.nice_to_have_skills = user_profile.get('skills', {}).get('nice_to_have', [])
            self.job_types = user_profile.get('job_types', [])
            self.locations = user_profile.get('locations', {}).get('countries', [])
            self.remote_preferred = user_profile.get('locations', {}).get('remote', False)
            self.min_salary = user_profile.get('min_salary', 0)
            self.excluded_terms = user_profile.get('excluded_terms', [])
            self.keywords = user_profile.get('keywords', [])

            weights = user_profile.get('scoring', {})
            self.skill_weight = weights.get('skill_match_weight', 0.4)
            self.location_weight = weights.get('location_weight', 0.2)
            self.salary_weight = weights.get('salary_weight', 0.2)
            self.company_weight = weights.get('company_weight', 0.1)
            self.description_weight = weights.get('job_description_weight', 0.1)

        def match_job(self, job: Dict[str, Any]) -> Dict[str, float]:
            skill_score = self._calculate_skill_score(job)
            location_score = self._calculate_location_score(job)
            salary_score = self._calculate_salary_score(job)
            company_score = self._calculate_company_score(job)
            description_score = self._calculate_description_score(job)

            match_score = (
                skill_score * self.skill_weight +
                location_score * self.location_weight +
                salary_score * self.salary_weight +
                company_score * self.company_weight +
                description_score * self.description_weight
            )

            return {
                'match_score': match_score,
                'skill_score': skill_score,
                'location_score': location_score,
                'salary_score': salary_score,
                'company_score': company_score,
                'description_score': description_score
            }

        def _calculate_skill_score(self, job: Dict[str, Any]) -> float:
            job_skills = job.get('skills', [])
            if isinstance(job_skills, str):
                job_skills = [s.strip() for s in job_skills.split(',')]

            job_skills_lower = [s.lower() for s in job_skills]

            must_have_matches = sum(1 for skill in self.must_have_skills
                                    if skill.lower() in job_skills_lower)

            nice_to_have_matches = sum(1 for skill in self.nice_to_have_skills
                                      if skill.lower() in job_skills_lower)

            if not self.must_have_skills:
                must_have_score = 1.0
            else:
                must_have_score = must_have_matches / len(self.must_have_skills)

            if not self.nice_to_have_skills:
                nice_to_have_score = 1.0
            else:
                nice_to_have_score = nice_to_have_matches / len(self.nice_to_have_skills)

            return 0.7 * must_have_score + 0.3 * nice_to_have_score

        def _calculate_location_score(self, job: Dict[str, Any]) -> float:
            is_remote = job.get('remote', False)
            location = str(job.get('location', '')).lower()

            if is_remote and self.remote_preferred:
                return 1.0

            location_match = any(loc.lower() in location for loc in self.locations)

            if location_match:
                return 0.8
            elif is_remote and not self.remote_preferred:
                return 0.5
            else:
                return 0.2

        def _calculate_salary_score(self, job: Dict[str, Any]) -> float:
            min_salary = job.get('salary_min', 0)
            max_salary = job.get('salary_max', 0)
            currency = job.get('salary_currency', '')

            if min_salary == 0 and max_salary == 0:
                return 0.5

            if currency == 'EUR':
                min_salary *= 1.1
            elif currency == 'GBP':
                min_salary *= 1.3

            if min_salary >= self.min_salary * 1.5:
                return 1.0
            elif min_salary >= self.min_salary * 1.2:
                return 0.9
            elif min_salary >= self.min_salary:
                return 0.8
            elif min_salary >= self.min_salary * 0.8:
                return 0.6
            elif min_salary > 0:
                return 0.4
            else:
                return 0.5

        def _calculate_company_score(self, job: Dict[str, Any]) -> float:
            return 0.7

        def _calculate_description_score(self, job: Dict[str, Any]) -> float:
            description = str(job.get('description', '')).lower()

            exclusion_matches = sum(1 for term in self.excluded_terms
                                   if term.lower() in description)

            keyword_matches = sum(1 for keyword in self.keywords
                                 if keyword.lower() in description)

            if not self.keywords:
                keyword_score = 0.7
            else:
                keyword_score = min(1.0, keyword_matches / len(self.keywords))

            if self.excluded_terms:
                exclusion_penalty = exclusion_matches / len(self.excluded_terms)
            else:
                exclusion_penalty = 0

            score = keyword_score * (1 - exclusion_penalty * 0.8)

            return max(0.1, score)