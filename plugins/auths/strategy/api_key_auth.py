import requests

from plugins.auths.strategy.auth_strategy import AuthStrategy

class APIKeyAuthStrategy(AuthStrategy):
    """
    Example strategy for APIs that require an API key in a header.
    """
    def __init__(self, api_key: str):
        self.api_key = api_key

    def authenticate(self, session: requests.Session) -> None:
        session.headers.update({"Authorization": f"Bearer {self.api_key}"})
