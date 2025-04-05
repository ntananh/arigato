"""
JobBoard - Abstract base class for all job board hooks
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional

class JobBoard(ABC):
    """
    Abstract base class defining the interface for job board hooks.

    All job board implementations should inherit from this class
    and implement its methods.
    """

    @abstractmethod
    def search_jobs(
        self,
        job_type: str,
        location: Optional[str] = None,
        keywords: Optional[List[str]] = None,
        max_results: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Search for jobs based on criteria.

        Parameters:
        -----------
        job_type : str
            The type of job to search for (e.g., "software engineer")
        location : Optional[str]
            Location to search in (e.g., "remote", "New York")
        keywords : Optional[List[str]]
            List of keywords to include in search
        max_results : int
            Maximum number of results to return

        Returns:
        --------
        List[Dict[str, Any]]
            List of job postings as dictionaries
        """
        pass