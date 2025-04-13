import os
import json
import logging
from typing import List, Dict, Any
from datetime import datetime, timedelta

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)

class DataCollectionTask(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        source: str,
        job_types: List[str],
        keywords: List[str],
        locations: List[str],
        max_results: int = 100,
        data_collection_id: str = "data_collection_id",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.source = source.lower()
        self.job_types = job_types
        self.keywords = keywords
        self.locations = locations
        self.max_results = max_results
        self.data_collection_id = data_collection_id

        # Initialize paths
        self.airflow_home = os.environ.get('AIRFLOW_HOME', '')
        self.data_dir = os.path.join(self.airflow_home, 'data/json')
        self.config_dir = os.path.join(self.airflow_home, 'include', 'config')

        # Ensure directories exist
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.config_dir, exist_ok=True)

    def execute(self, context):
        self._validate_parameters()

        logger.info(f"Starting collection from {self.source} with types: {self.job_types}")

        if self._should_use_cache():
            jobs = self._load_from_cache()
        else:
            jobs = self._collect_from_api()
            if jobs:
                self._save_to_cache(jobs)

        if not jobs:
            logger.warning(f"No jobs collected from {self.source}")
            return []

        context['ti'].xcom_push(key='return_value', value=jobs)
        context['ti'].xcom_push(key=f'{self.source}_jobs', value=jobs)
        logger.info(f"Pushed {len(jobs)} jobs to XCom with keys: 'return_value' and '{self.source}_jobs'")
        return jobs

    def _validate_parameters(self):
        """Validate all required parameters"""
        if not all([self.source, self.job_types, self.keywords, self.locations]):
            raise AirflowException(
                "Missing required parameters: source, job_types, keywords, and locations must all be provided"
            )

        if self.source not in ['linkedin', 'upwork', 'rapidapi_upwork']:
            raise AirflowException(f"Unsupported source: {self.source}")

    def _should_use_cache(self) -> bool:
        """Determine if we should use cached data"""
        if Variable.get('force_refresh', 'false').lower() == 'true':
            return False

        cache_file = self._get_cache_file_path()
        if not os.path.exists(cache_file):
            return False

        cache_max_age = int(Variable.get('cache_max_age_hours', '24'))
        file_mtime = datetime.fromtimestamp(os.path.getmtime(cache_file))
        return (datetime.now() - file_mtime) < timedelta(hours=cache_max_age)

    def _get_cache_file_path(self) -> str:
        """Get the path to the cache file for this source"""
        return os.path.join(self.data_dir, f"{self.source}_jobs.json")

    def _load_from_cache(self) -> List[Dict[str, Any]]:
        """Load jobs from cache file"""
        cache_file = self._get_cache_file_path()
        try:
            with open(cache_file, 'r') as f:
                jobs = json.load(f)
            logger.info(f"Loaded {len(jobs)} jobs from cache: {cache_file}")
            return jobs
        except Exception as e:
            logger.error(f"Error loading from cache: {str(e)}")
            return []

    def _save_to_cache(self, jobs: List[Dict[str, Any]]) -> None:
        """Save jobs to cache file"""
        cache_file = self._get_cache_file_path()
        try:
            with open(cache_file, 'w') as f:
                json.dump(jobs, f)
            logger.info(f"Saved {len(jobs)} jobs to cache: {cache_file}")
        except Exception as e:
            logger.error(f"Error saving to cache: {str(e)}")

    def _collect_from_api(self) -> List[Dict[str, Any]]:
        """Collect jobs from the specified API"""
        try:
            from job_board_hooks.rapid_api_hook import RapidApiGateway

            rapidapi_key = os.environ.get('RAPIDAPI_KEY')
            if not rapidapi_key:
                raise AirflowException("RAPIDAPI_KEY environment variable is not set")

            headers = self._load_api_headers()
            hook = RapidApiGateway(
                api_name=self.source,
                api_key=rapidapi_key,
                headers=headers.get('rapidapi', {})
            )

            collected_jobs = []
            for job_type in self.job_types:
                for location in self.locations:
                    logger.info(f"Searching for {job_type} jobs in {location}")
                    results = hook.search_jobs(
                        job_type=job_type,
                        location=location,
                        keywords=self.keywords,
                        max_results=self.max_results
                    )
                    if results:
                        collected_jobs.extend(results)
                        logger.info(f"Found {len(results)} jobs for {job_type} in {location}")

            return collected_jobs

        except ImportError as e:
            raise AirflowException(f"Could not import RapidApiGateway: {str(e)}")
        except Exception as e:
            raise AirflowException(f"Error collecting from {self.source}: {str(e)}")

    def _load_api_headers(self) -> Dict[str, Any]:
        """Load API headers configuration"""
        headers_path = os.path.join(self.config_dir, 'headers.json')
        try:
            with open(headers_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Error loading headers config: {str(e)}")
            return {}