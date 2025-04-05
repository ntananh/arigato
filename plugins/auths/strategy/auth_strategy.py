"""
AuthStrategy - Abstract base class for authentication strategies
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import requests

class AuthStrategy(ABC):
    """
    Abstract base class for authentication strategies.

    Different APIs require different authentication methods.
    This class provides a common interface for all authentication strategies.
    """

    @abstractmethod
    def authenticate(self, request: requests.Request) -> requests.Request:
        """
        Add authentication to the request.

        Parameters:
        -----------
        request : requests.Request
            The request to authenticate

        Returns:
        --------
        requests.Request
            The authenticated request
        """
        pass

    @abstractmethod
    def refresh_auth(self) -> bool:
        """
        Refresh authentication credentials if needed.

        Returns:
        --------
        bool
            True if refresh was successful, False otherwise
        """
        pass