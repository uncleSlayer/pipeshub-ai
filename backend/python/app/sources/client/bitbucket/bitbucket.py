import base64
import logging
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional
from urllib.parse import urlencode

from app.config.configuration_service import ConfigurationService
from app.config.constants.http_status_code import HttpStatusCode
from app.sources.client.http.http_client import HTTPClient, HTTPRequest
from app.sources.client.iclient import IClient


class BitbucketRESTClientViaBasicAuth(HTTPClient):
    """Bitbucket Cloud REST client via Basic Auth (User API Token or App Password)

    This is the standard authentication method for User API Tokens in Bitbucket Cloud.
    The client handles the Base64 encoding of 'username:password' automatically.

    Args:
        base_url: The base URL of the Bitbucket instance (usually https://api.bitbucket.org/2.0)
        username: The Bitbucket username (or email for API Tokens)
        password: The App Password or API Token value
    """

    def __init__(self, base_url: str, username: str, password: str) -> None:
        # Construct the auth string "username:password"
        auth_str = f"{username}:{password}"
        # Base64 encode it for the Basic Auth header
        encoded_auth = base64.b64encode(auth_str.encode("ascii")).decode("ascii")

        # Initialize HTTPClient with "Basic" type and the encoded string
        super().__init__(encoded_auth, "Basic")
        self.base_url = base_url.rstrip("/")

    def get_base_url(self) -> str:
        """Get the base URL"""
        return self.base_url


class BitbucketRESTClientViaBearer(HTTPClient):
    """Bitbucket Cloud REST client via Bearer Token (Workspace Access Token / OAuth)

    This is the standard method for Workspace Access Tokens and OAuth integrations.

    Args:
        base_url: The base URL of the Bitbucket instance
        token: The Access Token (Workspace or OAuth)
    """

    def __init__(self, base_url: str, token: str) -> None:
        super().__init__(token, "Bearer")
        self.base_url = base_url.rstrip("/")

    def get_base_url(self) -> str:
        """Get the base URL"""
        return self.base_url


class BitbucketRESTClientViaOAuth(HTTPClient):
    """Bitbucket REST client via OAuth 2.0 - handles OAuth flow internally."""

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        redirect_uri: str,
        access_token: Optional[str] = None,
        refresh_token: Optional[str] = None,
        base_url: str = "https://api.bitbucket.org/2.0",
    ) -> None:
        # Initialize with empty token first if not provided
        super().__init__(access_token or "", "Bearer")

        self.base_url = base_url.rstrip("/")
        self.oauth_base_url = "https://bitbucket.org/site/oauth2"
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.refresh_token_val = refresh_token

        # Track if we have a valid token
        self._oauth_completed = access_token is not None

    def get_base_url(self) -> str:
        return self.base_url

    def is_oauth_completed(self) -> bool:
        return self._oauth_completed

    def get_authorization_url(self, state: Optional[str] = None) -> str:
        """Generate OAuth authorization URL"""
        params = {
            "client_id": self.client_id,
            "response_type": "code",
            "redirect_uri": self.redirect_uri,
        }

        if state:
            params["state"] = state

        return f"{self.oauth_base_url}/authorize?{urlencode(params)}"

    async def initiate_oauth_flow(self, authorization_code: str) -> Optional[str]:
        """Complete OAuth flow with authorization code"""
        return await self._exchange_code_for_token(authorization_code)

    async def refresh_token(self) -> Optional[str]:
        """Refresh OAuth access token using the stored refresh token"""
        if not self.refresh_token_val:
            raise ValueError("No refresh token available")

        return await self._execute_token_request(
            {"grant_type": "refresh_token", "refresh_token": self.refresh_token_val}
        )

    async def _exchange_code_for_token(self, code: str) -> Optional[str]:
        """Exchange authorization code for access token"""
        return await self._execute_token_request(
            {
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": self.redirect_uri,  # Bitbucket requires redirect_uri on exchange if used in auth
            }
        )

    async def _execute_token_request(self, data: Dict[str, str]) -> Optional[str]:
        """Internal helper to execute token requests (exchange or refresh)"""

        # Prepare Basic Auth for the token endpoint
        credentials = f"{self.client_id}:{self.client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()

        headers = {
            "Authorization": f"Basic {encoded_credentials}",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        # Bitbucket expects form-urlencoded body
        body_str = urlencode(data)

        request = HTTPRequest(
            method="POST",
            url=f"{self.oauth_base_url}/access_token",
            headers=headers,
            body=body_str,
        )

        # Use a temporary client or the parent logic to execute, but we need
        # to ensure we don't use the 'Bearer' token for this specific request
        # because we are using 'Basic' auth in the headers manually.
        # We create a raw request using the base HTTPClient logic or a temporary instance.
        # Since this class IS an HTTPClient, we can use self.execute but strictly overriding headers.

        response = await self.execute(request)

        if response.status >= HttpStatusCode.BAD_REQUEST.value:
            # Add error logging here if needed
            raise Exception(
                f"Token request failed with status {response.status}: {response.text}"
            )

        token_data = response.json()

        self.access_token = token_data.get("access_token")
        self.refresh_token_val = token_data.get("refresh_token")

        if self.access_token:
            # Update the main client headers for future API calls
            self.headers["Authorization"] = f"Bearer {self.access_token}"
            self._oauth_completed = True

        return self.access_token


@dataclass
class BitbucketOAuthConfig:
    """Configuration for Bitbucket client via OAuth"""

    client_id: str
    client_secret: str
    redirect_uri: str
    access_token: Optional[str] = None
    refresh_token: Optional[str] = None
    base_url: str = "https://api.bitbucket.org/2.0"

    def create_client(self) -> BitbucketRESTClientViaOAuth:
        return BitbucketRESTClientViaOAuth(
            client_id=self.client_id,
            client_secret=self.client_secret,
            redirect_uri=self.redirect_uri,
            access_token=self.access_token,
            refresh_token=self.refresh_token,
            base_url=self.base_url,
        )


@dataclass
class BitbucketBasicAuthConfig:
    """Configuration for Bitbucket client via Basic Auth (User API Token)

    Args:
        username: The Bitbucket username or email address
        password: The API Token or App Password
        base_url: The base URL (default: https://api.bitbucket.org/2.0)
    """

    username: str
    password: str
    base_url: str = "https://api.bitbucket.org/2.0"

    def create_client(self) -> BitbucketRESTClientViaBasicAuth:
        return BitbucketRESTClientViaBasicAuth(
            self.base_url, self.username, self.password
        )

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class BitbucketTokenConfig:
    """Configuration for Bitbucket client via Bearer Token (Workspace Token)

    Args:
        token: The Workspace Access Token or OAuth Access Token
        base_url: The base URL (default: https://api.bitbucket.org/2.0)
    """

    token: str
    base_url: str = "https://api.bitbucket.org/2.0"

    def create_client(self) -> BitbucketRESTClientViaBearer:
        return BitbucketRESTClientViaBearer(self.base_url, self.token)

    def to_dict(self) -> dict:
        return asdict(self)


class BitbucketClient(IClient):
    """Builder class for Bitbucket clients"""

    def __init__(
        self, client: BitbucketRESTClientViaBasicAuth | BitbucketRESTClientViaBearer
    ) -> None:
        """Initialize with a Bitbucket client object"""
        self.client = client

    def get_client(
        self,
    ) -> BitbucketRESTClientViaBasicAuth | BitbucketRESTClientViaBearer:
        """Return the Bitbucket client object"""
        return self.client

    def get_base_url(self) -> str:
        """Return the base URL"""
        return self.client.get_base_url()

    @classmethod
    def build_with_config(
        cls, config: BitbucketBasicAuthConfig | BitbucketTokenConfig
    ) -> "BitbucketClient":
        """Build BitbucketClient with configuration"""
        return cls(config.create_client())

    @classmethod
    async def build_from_services(
        cls,
        logger: logging.Logger,
        config_service: ConfigurationService,
    ) -> "BitbucketClient":
        """Build BitbucketClient using configuration service

        Supports two authentication strategies:
        1. BASIC: For User API Tokens (requires email/username + token)
        2. BEARER: For Workspace Access Tokens (requires token only)

        Args:
            logger: Logger instance
            config_service: Configuration service instance
        Returns:
            BitbucketClient instance
        """
        try:
            # Get Bitbucket configuration from the configuration service
            config_data = await cls._get_connector_config(logger, config_service)
            if not config_data:
                raise ValueError("Failed to get Bitbucket connector configuration")

            auth_config = config_data.get("auth", {}) or {}
            if not auth_config:
                raise ValueError(
                    "Auth configuration not found in Bitbucket connector configuration"
                )

            # Extract configuration values
            auth_type = auth_config.get("authType", "BASIC").upper()
            base_url = auth_config.get("baseUrl", "https://api.bitbucket.org/2.0")

            if auth_type == "BASIC" or auth_type == "USERNAME_PASSWORD":
                # Handle User API Token (Email + Token) or App Password
                username = auth_config.get("username", "")  # Or email
                password = auth_config.get("password", "")  # Or API Token

                if not username or not password:
                    raise ValueError(
                        "Username (or email) and password (or API token) required for BASIC auth type"
                    )

                config = BitbucketBasicAuthConfig(
                    username=username, password=password, base_url=base_url
                )

            elif auth_type == "BEARER" or auth_type == "TOKEN":
                # Handle Workspace Access Token or OAuth Token
                token = auth_config.get("token", "")

                if not token:
                    raise ValueError("Token required for BEARER auth type")

                config = BitbucketTokenConfig(token=token, base_url=base_url)

            else:
                raise ValueError(
                    f"Invalid auth type: {auth_type}. Supported types: BASIC, BEARER"
                )

            return cls.build_with_config(config)

        except Exception as e:
            logger.error(f"Failed to build Bitbucket client from services: {str(e)}")
            raise

    @staticmethod
    async def _get_connector_config(
        logger: logging.Logger, config_service: ConfigurationService
    ) -> Dict[str, Any]:
        """Fetch connector config from etcd for Bitbucket."""
        try:
            config = await config_service.get_config(
                "/services/connectors/bitbucket/config"
            )
            return config or {}
        except Exception as e:
            logger.error(f"Failed to get Bitbucket connector config: {e}")
            return {}
