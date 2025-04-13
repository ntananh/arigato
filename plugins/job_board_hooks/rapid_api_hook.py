import os
import json
import logging
from typing import Dict, Any, List, Optional

from urllib.parse import urlencode

import requests

from plugins.job_board_hooks.job_board import JobBoard
from plugins.auths.strategy.api_key_auth import ApiKeyAuth
from plugins.auths.strategy.auth_strategy import AuthStrategy

class RapidApiGateway(JobBoard):
    API_CONFIGS = {
        'linkedin': {
            'host': 'linkedin-job-search-api.p.rapidapi.com',
            'base_url': '/active-jb-7d',
            'output_file': 'data/json/linkedin_jobs.json'
        },
        'upwork': {
            'host': 'upwork-jobs-api2.p.rapidapi.com',
            'base_url': '/active-freelance-24h',
            'output_file': 'data/json/upwork_jobs.json'
        }
    }

    def __init__(
        self,
        api_name: str,
        api_key: Optional[str] = None,
        auth_strategy: Optional[AuthStrategy] = None,
        headers: Optional[Dict[str, str]] = None
    ):
        if api_name.lower() not in self.API_CONFIGS:
            raise ValueError(
                f"Unsupported API: {api_name}. Supported APIs: {list(self.API_CONFIGS.keys())}")

        api_config = self.API_CONFIGS[api_name.lower()]

        self.api_key = api_key or os.environ.get('RAPIDAPI_KEY')
        if not self.api_key:
            raise ValueError(
                "RapidAPI key is required. Set RAPIDAPI_KEY environment variable.")

        self.headers = headers or {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9"
        }

        rapid_api_headers = {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": api_config['host']
        }
        self.headers.update(rapid_api_headers)

        self.auth_strategy = auth_strategy or ApiKeyAuth(
            api_key=self.api_key,
            key_name="X-RapidAPI-Key",
            location="header",
            headers=self.headers
        )

        self.api_name = api_name.lower()
        self.base_url = f"https://{api_config['host']}{api_config['base_url']}"
        self.output_file = api_config['output_file']

        self.logger = logging.getLogger(__name__)

    def search_jobs(
        self,
        job_type: str,
        location: Optional[str] = None,
        keywords: Optional[List[str]] = None,
        max_results: int = 100
    ) -> List[Dict[str, Any]]:
        self.logger.info(f"Searching {self.api_name} jobs for {job_type}")

        params = self._build_search_params(
            job_type, location, keywords, max_results)

        raw_data = self._make_api_request(params)

        self._save_raw_data(raw_data)

        self.logger.info(f"Saved {len(raw_data)} jobs to {self.output_file}")
        return raw_data

    def _build_search_params(
        self,
        job_type: str,
        location: Optional[str] = None,
        keywords: Optional[List[str]] = None,
        max_results: int = 100
    ) -> Dict[str, Any]:
        if self.api_name == 'linkedin':
            return self._build_linkedin_params(job_type, location, keywords, max_results)
        elif self.api_name == 'upwork':
            return self._build_upwork_params(job_type, location, keywords, max_results)
        else:
            return {
                'query': job_type,
                'limit': max_results,
                'location': location or ''
            }

    def _build_linkedin_params(self, job_type, location, keywords, max_results):
        query = f'title_filter="{job_type}"'
        if location:
            query += f' location_filter="{location}"'
        if keywords:
            query += ' ' + ' '.join([f'"{kw}"' for kw in keywords])

        return {
            'limit': max_results,
            'offset': 0,
            'query': query
        }

    def _build_upwork_params(self, job_type, location, keywords, max_results):
        query = job_type
        if keywords:
            query += ' ' + ' '.join(keywords)
        if location:
            query += f' {location}'

        return {
            'query': query,
            'page': '1',
            'num_pages': str(max(1, max_results // 10))
        }

    def _make_api_request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}?{urlencode(params)}"
        request = requests.Request("GET", url, headers=self.headers)
        request = self.auth_strategy.authenticate(request)

        with requests.Session() as session:
            try:
                prepped = session.prepare_request(request)
                response = session.send(prepped)
                response.raise_for_status()

                return response.json()

            except requests.exceptions.RequestException as e:
                self.logger.error(
                    f"Error fetching jobs via RapidAPI: {str(e)}")
                return {}

    def _save_raw_data(self, data: Dict[str, Any]):
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)

        with open(self.output_file, 'w') as f:
            json.dump(data, f, indent=2)

    @classmethod
    def create_hook(
        cls,
        api_name: str,
        api_key: Optional[str] = None
    ) -> 'RapidApiGateway':
        return cls(
            api_name=api_name,
            api_key=api_key
        )
