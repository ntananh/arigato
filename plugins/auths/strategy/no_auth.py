"""
NoAuth - Authentication strategy for APIs that don't require authentication
"""
import requests
from typing import Dict, Any, Optional

from plugins.auths.strategy.auth_strategy import AuthStrategy

class NoAuth(AuthStrategy):
    """
    Authentication strategy for APIs that don't require authentication.
    """

    def __init__(self, headers: Optional[Dict[str, str]] = None):
        """
        Initialize the NoAuth strategy.

        Parameters:
        -----------
        headers : Optional[Dict[str, str]]
            Optional headers to include in all requests
        """
        self.headers = headers or {}

    def authenticate(self, request: requests.Request) -> requests.Request:
        """
        Add headers to the request.

        Parameters:
        -----------
        request : requests.Request
            The request to authenticate

        Returns:
        --------
        requests.Request
            The request with headers added
        """
        # Add headers to the request
        for key, value in self.headers.items():
            request.headers[key] = value

        return request

    def refresh_auth(self) -> bool:
        """
        NoAuth doesn't need refreshing.

        Returns:
        --------
        bool
            Always returns True
        """
        return True