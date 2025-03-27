
import requests

from plugins.auths.strategy.auth_strategy import AuthStrategy

class NoAuthStrategy(AuthStrategy):
    """
    A no-op authentication strategy for public APIs or endpoints
    that do not require authenticated access.
    """
    def authenticate(self, session: requests.Session) -> None:
        # No authentication steps needed
        pass



