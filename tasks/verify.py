import os
import logging

from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults

logger = logging.getLogger(__name__)

class VerificationTask(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(__name__)

    def execute(self, context):
        try:
            self._verify_variables()
            self._verify_directories()
            self._verify_api_keys()
            logger.info("-----ALL VERIFICATIONS PASSED-----")

            # Collect and return configuration for downstream tasks
            config = {
                'job_types': Variable.get('job_types', '["software engineer", "data scientist"]', deserialize_json=True),
                'keywords': Variable.get('keywords', '["python", "machine learning"]', deserialize_json=True),
                'locations': Variable.get('locations', '["remote", "san francisco"]', deserialize_json=True),
                'max_results': int(Variable.get('max_results', '50')),
            }

            # Push to XCom for downstream tasks
            context['ti'].xcom_push(key='config', value=config)

            return True
        except Exception as e:
            logger.error(f"Verification failed: {str(e)}")
            raise

    def _verify_variables(self) -> None:
        required_variables = [
            'job_types',
            'keywords',
            'locations',
            'max_results',
            'data_sources',
            'notification_email'
        ]
        missing_variables = []
        for var in required_variables:
            try:
                Variable.get(var)
            except Exception:
                missing_variables.append(var)
        if missing_variables:
            logger.warning(f"Missing required variables: {missing_variables}. Using defaults.")

    def _verify_directories(self) -> None:
        required_dirs = [
            os.path.join(os.environ.get('AIRFLOW_HOME', ''), 'data'),
            os.path.join(os.environ.get('AIRFLOW_HOME', ''), 'include', 'config'),
        ]
        for directory in required_dirs:
            if not os.path.exists(directory):
                logger.warning(f"Creating directory: {directory}")
                os.makedirs(directory, exist_ok=True)

    def _verify_api_keys(self) -> None:
        required_api_keys = []
        data_sources = Variable.get('data_sources', '["linkedin", "upwork"]', deserialize_json=True)
        if 'upwork' in data_sources or 'rapidapi_upwork' in data_sources:
            required_api_keys.append('RAPIDAPI_KEY')
        for key in required_api_keys:
            if not os.environ.get(key):
                logger.warning(f"Missing required API key: {key}. Some data sources may not be available.")