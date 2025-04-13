import logging
import yaml
from pathlib import Path

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadPreferenceTask(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        preferences_path: str = "include/config/job_preferences.yaml",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(__name__)
        self.project_root = Path(__file__).parent.parent
        self.preferences_path = self.project_root / preferences_path

    def execute(self, context):
        self.logger.info(f"Loading user preferences from {self.preferences_path}")
        try:
            if not self.preferences_path.exists():
                self.logger.error(f"Preferences file not found: {self.preferences_path}")
                raise FileNotFoundError(f"Preferences file not found: {self.preferences_path}")

            with open(self.preferences_path, 'r') as file:
                preferences = yaml.safe_load(file)
                self.logger.debug(f"Loaded preferences: {preferences}")

                context['ti'].xcom_push(key='preferences', value=preferences)

                return preferences

        except FileNotFoundError as e:
            self.logger.error(f"Failed to find preferences file: {str(e)}")
            raise
        except yaml.YAMLError as e:
            self.logger.error(f"Failed to parse preferences YAML: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error loading preferences: {str(e)}")
            raise