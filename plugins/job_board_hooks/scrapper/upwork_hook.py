"""
UpworkHook - Hook for accessing Upwork Jobs API
"""
import logging
from typing import Dict, Any, List, Optional
import requests
from urllib.parse import urlencode

from plugins.job_board_hooks.job_board import JobBoard
from plugins.auths.strategy.no_auth import NoAuth
from plugins.auths.strategy.api_key_auth import ApiKeyAuth
from plugins.auths.strategy.auth_strategy import AuthStrategy


class UpworkHook(JobBoard):

    def __init__(
        self,
        api_key: Optional[str] = None,
        auth_strategy: Optional[AuthStrategy] = None,
        headers: Optional[Dict[str, str]] = None,
        base_url: str = "https://www.upwork.com/api/jobs/v2/search"
    ):
        self.headers = headers or {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9"
        }

        if api_key and not auth_strategy:
            self.auth_strategy = ApiKeyAuth(
                api_key=api_key,
                key_name="X-Upwork-API-Key",
                location="header",
                headers=self.headers
            )
        else:
            self.auth_strategy = auth_strategy or NoAuth(headers=self.headers)

        self.base_url = base_url

        self.logger = logging.getLogger(__name__)

    def search_jobs(
        self,
        job_type: str,
        location: Optional[str] = None,
        keywords: Optional[List[str]] = None,
        max_results: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Search for jobs on Upwork.

        Parameters:
        -----------
        job_type : str
            The type of job to search for (e.g., "python developer")
        location : Optional[str]
            Not used for Upwork (all jobs are remote)
        keywords : Optional[List[str]]
            List of keywords to include in search
        max_results : int
            Maximum number of results to return

        Returns:
        --------
        List[Dict[str, Any]]
            List of job postings
        """
        self.logger.info(f"Searching Upwork jobs for {job_type}")

        # Build the query parameters
        params = {
            "q": job_type,
            "page": 1,
                "per_page": max_results
            }

        if keywords:
            params["q"] += " " + " ".join(keywords)
            # Make the API request
            jobs = self._make_api_request(params)

        self.logger.info(f"Found {len(jobs)} Upwork jobs")

        return jobs

    def get_job_details(self, job_id: str) -> Dict[str, Any]:
        self.logger.info(f"Getting details for Upwork job {job_id}")

        url = f"https://www.upwork.com/api/jobs/v2/jobs/{job_id}"

        request = requests.Request("GET", url)
        request = self.auth_strategy.authenticate(request)

        with requests.Session() as session:
            try:
                prepped = session.prepare_request(request)
                response = session.send(prepped)
                response.raise_for_status()
                job_details = response.json()
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Error fetching Upwork job details: {str(e)}")
                job_details = {}

        return job_details

    def _make_api_request(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        url = f"{self.base_url}?{urlencode(params)}"
        request = requests.Request("GET", url)
        request = self.auth_strategy.authenticate(request)

        with requests.Session() as session:
            try:
                prepped = session.prepare_request(request)
                response = session.send(prepped)
                response.raise_for_status()

                data = response.json()

                jobs = data.get("jobs", [])

                jobs = jobs[:params.get("per_page", 50)]

                return jobs
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Error fetching Upwork jobs: {str(e)}")
                return []