import requests
from abc import ABC, abstractmethod

class AuthStrategy(ABC):
    """
    Interface for authentication strategies.
    Concrete implementations might handle:
      - No authentication
      - API key header
      - OAuth token retrieval
      - etc.
    """

    @abstractmethod
    def authenticate(self, session: requests.Session) -> None:
        """
        Apply any required authentication to the given session.
        """
        pass
