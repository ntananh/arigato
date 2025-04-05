import json
import math
import random
import time
import requests
from bs4 import BeautifulSoup
from typing import Any, Dict, List, Optional

from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin

class LinkedinHook(BaseHook, LoggingMixin):
    """
    LinkedIn Hook for Airflow to interact with LinkedIn job search.
    Performs web scraping of LinkedIn job listings.

    :param linkedin_conn_id: Connection ID for LinkedIn (if needed)
    :param keywords: Job search keywords
    :param location: Job search location
    """

    def __init__(
        self,
        linkedin_conn_id: Optional[str] = None,
        keywords: str = "Java (Programming Language)",
        location: str = "Finland, European Union",
        *args, **kwargs
    ):
        BaseHook.__init__(self, *args, **kwargs)
        LoggingMixin.__init__(self)

        self.linkedin_conn_id = linkedin_conn_id
        self.keywords = keywords.replace(" ", "%20")
        self.location = location.replace(" ", "%20")

        self.base_search_url = 'https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search'
        self.job_details_url = 'https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{}'

        headers_file = open("include/config/headers.json")
        self.headers = dict(json.load(headers_file))

    def get_conn(self) -> requests.Session:
        """
        Create a connection to LinkedIn.
        :return: Requests session
        """
        session = requests.Session()
        session.headers.update(self.headers)

        if self.linkedin_conn_id:
            try:
                conn = self.get_connection(self.linkedin_conn_id)
                # Add authentication logic here if needed
            except Exception as e:
                raise AirflowException(f"Error getting connection: {e}")

        return session

    def _get_with_backoff(self, session: requests.Session, url: str, max_retries: int = 5) -> requests.Response:
        """
        Make a GET request with exponential backoff on 429 responses.
        :param session: Requests session
        :param url: URL to request
        :param max_retries: Maximum number of retries
        :return: HTTP response if successful
        :raises AirflowException: if max retries exceeded
        """
        retries = 0
        backoff = 1  # initial delay in seconds
        while retries < max_retries:
            response = session.get(url, timeout=10)
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", backoff))
                self.log.warning(f"429 Too Many Requests for URL {url}. Waiting {retry_after} seconds (retry {retries + 1}/{max_retries})")
                time.sleep(retry_after)
                retries += 1
                backoff *= 2  # Exponential backoff
            else:
                try:
                    response.raise_for_status()
                    return response
                except requests.HTTPError as e:
                    self.log.error(f"HTTP error for URL {url}: {e}")
                    raise
        raise AirflowException(f"Max retries exceeded for URL: {url}")

    def _get_job_ids(self, session: requests.Session, max_pages: int = 5) -> List[str]:
        """
        Fetch job IDs from LinkedIn search results.
        :param session: Requests session
        :param max_pages: Maximum number of pages to scrape
        :return: List of job IDs
        """
        job_ids = []
        for page in range(max_pages):
            search_url = f"{self.base_search_url}?keywords={self.keywords}&location={self.location}&start={page * 25}"
            try:
                response = self._get_with_backoff(session, search_url)
                soup = BeautifulSoup(response.text, 'html.parser')
                page_jobs = soup.find_all("li")
                for job in page_jobs:
                    try:
                        job_id = job.find("div", {"class": "base-card"}).get('data-entity-urn').split(":")[3]
                        job_ids.append(job_id)
                    except Exception as e:
                        self.log.warning(f"Could not extract job ID: {e}")
                self.log.info(f"Page {page + 1}: Found {len(page_jobs)} jobs")
            except requests.RequestException as e:
                self.log.error(f"Error fetching job search page {page}: {e}")
                break
        return job_ids

    def extract_job_details(self, job_id: str, session: requests.Session) -> Dict[str, Any]:
        """
        Extract details for a specific job.
        :param job_id: Job ID to fetch details for
        :param session: Requests session
        :return: Dictionary of job details
        """
        try:
            self.log.debug(f"Extracing job details for: #{job_id}")

            url = self.job_details_url.format(job_id)
            response = self._get_with_backoff(session, url)
            soup = BeautifulSoup(response.text, 'html.parser')
            job_info: Dict[str, Optional[str]] = {
                "job_id": job_id,
                "company": None,
                "job_title": None,
                "level": None,
                "location": None,
                "description": None,
                "posted_at": None,
                "updated_at": None,
                "crawled_at": None,
                "job_function": None,
                "employment_type": None,
                "applicants": None,
                "industries": None,
            }
            try:
                job_info["company"] = soup.find("div", {"class": "top-card-layout__card"}).find("a").find("img").get('alt')
            except Exception:
                self.log.warning(f"Could not extract company for job {job_id}")
            try:
                job_info["job_title"] = soup.find("div", {"class": "top-card-layout__entity-info"}).find("a").text.strip()
            except Exception:
                self.log.warning(f"Could not extract job title for job {job_id}")
            try:
                job_info["level"] = soup.find("ul", {"class": "description__job-criteria-list"}).find("li").text.replace("Seniority level", "").strip()
            except Exception:
                self.log.warning(f"Could not extract seniority level for job {job_id}")
            try:
                location_span = soup.find("span", class_="topcard__flavor topcard__flavor--bullet")
                if location_span:
                    job_info["location"] = location_span.get_text(strip=True)
                else:
                    self.log.warning(f"Could not extract location for job {job_id}")
            except Exception:
                self.log.warning(f"Could not extract seniority level for job {job_id}")

            try:
                description_section = soup.find("section", class_="core-section-container my-3 description")
                if description_section:
                    rich_text_div = description_section.find("div", class_="description__text description__text--rich")
                    if rich_text_div:
                        job_info["description"] = rich_text_div.get_text(separator="\n", strip=True)
                else:
                    self.log.warning(f"Could not extract location for job {job_id}")
            except Exception:
                self.log.warning(f"Could not extract seniority level for job {job_id}")

            self.log.debug(f"Got job details for: #{job_id}")
            return job_info
        except requests.RequestException as e:
            self.log.error(f"Error fetching job details for {job_id}: {e}")
            return {}

    def run(self, *args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
        """
        Primary method to run the hook and fetch job details.
        :return: List of job details
        """
        try:
            session = self.get_conn()
            job_ids = self._get_job_ids(session, max_pages=5)
            self.log.info(f"Total job IDs found: {len(job_ids)}")
            job_details = []
            for job_id in job_ids:
                job_info = self.extract_job_details(job_id, session)
                if job_info:
                    job_details.append(job_info)

                random_step = random.randrange(1, 3, 1);
                random_sleep_time = random.randrange(10, 20, random_step)

                self.log.info(f"Sleeping for: {random_sleep_time}s")

                time.sleep(random_sleep_time)
            return job_details
        except Exception as e:
            self.log.error(f"Error in LinkedIn job scraping: {e}")
            raise AirflowException(f"Failed to fetch LinkedIn jobs: {e}")
