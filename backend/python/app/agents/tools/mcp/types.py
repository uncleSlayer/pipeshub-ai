"""MCP connection types."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class McpAuthType(str, Enum):
    OAUTH = "oauth"
    BEARER_TOKEN = "bearer_token"


@dataclass
class McpConnectionInfo:
    """Decrypted MCP server connection info (from etcd)."""

    server_url: str
    access_token: str
    auth_type: McpAuthType = McpAuthType.BEARER_TOKEN
    refresh_token: str | None = None
    token_endpoint: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
    expires_at: int | None = None
    resource: str | None = None

    @classmethod
    def from_stored_token(cls, data: dict[str, Any]) -> "McpConnectionInfo":
        """Create from the StoredTokenData dict stored in etcd (after decryption)."""
        return cls(
            server_url=data["mcpServerUrl"],
            access_token=data["accessToken"],
            auth_type=McpAuthType(data.get("authType", "bearer_token")),
            refresh_token=data.get("refreshToken"),
            token_endpoint=data.get("tokenEndpoint"),
            client_id=data.get("clientId"),
            client_secret=data.get("clientSecret"),
            expires_at=data.get("expiresAt"),
            resource=data.get("resource"),
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize for passing through ChatState."""
        return {
            "server_url": self.server_url,
            "access_token": self.access_token,
            "auth_type": self.auth_type.value,
            "refresh_token": self.refresh_token,
            "token_endpoint": self.token_endpoint,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "expires_at": self.expires_at,
            "resource": self.resource,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "McpConnectionInfo":
        """Deserialize from ChatState dict."""
        return cls(
            server_url=data["server_url"],
            access_token=data["access_token"],
            auth_type=McpAuthType(data.get("auth_type", "bearer_token")),
            refresh_token=data.get("refresh_token"),
            token_endpoint=data.get("token_endpoint"),
            client_id=data.get("client_id"),
            client_secret=data.get("client_secret"),
            expires_at=data.get("expires_at"),
            resource=data.get("resource"),
        )
