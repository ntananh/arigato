"""
ApiKeyAuth - Authentication strategy for APIs that use API keys
"""
import os
import requests
from typing import Dict, Any, Optional

from plugins.auths.strategy.auth_strategy import AuthStrategy

class ApiKeyAuth(AuthStrategy):
    """
    Authentication strategy for APIs that use API keys.
    """

    def __init__(
        self,
        api_key: str,
        key_name: str = "api-key",
        location: str = "header",
        headers: Optional[Dict[str, str]] = None
    ):
        """
        Initialize the ApiKeyAuth strategy.

        Parameters:
        -----------
        api_key : str
            The API key to use for authentication
        key_name : str
            The name of the API key parameter (default: "api-key")
        location : str
            Where to put the API key: "header", "query", or "cookie"
        headers : Optional[Dict[str, str]]
            Optional headers to include in all requests
        """
        self.api_key = api_key
        self.key_name = key_name
        self.location = location.lower()
        self.headers = headers or {}

        if self.location not in ["header", "query", "cookie"]:
            raise ValueError("Location must be one of: header, query, cookie")

    def authenticate(self, request: requests.Request) -> requests.Request:
        """
        Add API key to the request.

        Parameters:
        -----------
        request : requests.Request
            The request to authenticate

        Returns:
        --------
        requests.Request
            The authenticated request
        """
        # Add headers
        for key, value in self.headers.items():
            request.headers[key] = value

        # Add API key based on location
        if self.location == "header":
            request.headers[self.key_name] = self.api_key
        elif self.location == "query":
            request.params = request.params or {}
            request.params[self.key_name] = self.api_key
        elif self.location == "cookie":
            request.cookies = request.cookies or {}
            request.cookies[self.key_name] = self.api_key

        return request

    def refresh_auth(self) -> bool:
        """
        API keys typically don't need refreshing.

        Returns:
        --------
        bool
            Always returns True
        """
        return True