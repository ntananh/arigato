import requests

from abc import ABC, abstractmethod

from plugins.auths.strategy.auth_strategy import AuthStrategy

class JobBoardHook(ABC):
    """
    Abstract base class for hooks that fetch job postings from
    various job boards or APIs.
    """

    def __init__(self, auth_strategy: AuthStrategy):
        self.auth_strategy = auth_strategy
        self.session = requests.Session()
        self.auth_strategy.authenticate(self.session)

    @abstractmethod
    def fetch_jobs(self) -> list:
        """
        Fetch job postings from the job board.
        Returns:
            list of job postings (dicts, JSON objects, or similar).
        """
        pass
