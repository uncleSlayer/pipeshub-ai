"""MCP etcd path constants and utilities.

Mirrors the Node.js MCP client's key structure at:
backend/nodejs/apps/src/modules/mcp_client/services/mcp-client.service.ts
"""

import hashlib

# Marker to identify MCP entries in the agent's toolsets array
MCP_TOOLSET_TYPE = "mcp"

# Timeout for connecting to / discovering tools from a single MCP server
MCP_DISCOVERY_TIMEOUT_SECONDS = 15

# Timeout for a single tool call
MCP_TOOL_CALL_TIMEOUT_SECONDS = 30


def hash_server_url(url: str) -> str:
    """Hash a server URL to produce the etcd key component.

    Mirrors Node's hashUrl(): SHA-256, first 16 hex chars.
    See mcp-client.service.ts line 536-538.
    """
    return hashlib.sha256(url.encode()).hexdigest()[:16]


def get_mcp_token_path(org_id: str, user_id: str, server_url_hash: str) -> str:
    """Build the etcd key for a stored MCP token.

    Path: /orgs/{orgId}/users/{userId}/mcp/{serverUrlHash}/token
    """
    return f"/orgs/{org_id}/users/{user_id}/mcp/{server_url_hash}/token"


def get_mcp_user_connections_prefix(org_id: str, user_id: str) -> str:
    """Build the etcd prefix to list all MCP connections for a user.

    Path: /orgs/{orgId}/users/{userId}/mcp/
    """
    return f"/orgs/{org_id}/users/{user_id}/mcp/"
