"""MCP session manager — handles connections to MCP servers."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import httpx
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client
from mcp.types import CallToolResult, Tool as McpTool

from app.agents.tools.mcp.constants import (
    MCP_TOOL_CALL_TIMEOUT_SECONDS,
    get_mcp_token_path,
    hash_server_url,
)
from app.agents.tools.mcp.types import McpAuthType, McpConnectionInfo

if TYPE_CHECKING:
    from app.config.configuration_service import ConfigurationService
    from app.config.encryption.encryption_service import EncryptionService

# Refresh tokens 60 seconds before actual expiry to avoid edge-case failures
_EXPIRY_BUFFER_MS = 60_000
_REFRESH_TIMEOUT_SECONDS = 10


@dataclass
class TokenRefreshContext:
    """Bundles the services and identity needed to refresh and persist OAuth tokens."""

    config_service: ConfigurationService
    encryption_service: EncryptionService
    org_id: str
    user_id: str


@dataclass
class _SessionEntry:
    """A live MCP session owned by a dedicated background task.

    The ``owner_task`` is the only task that ever enters or exits the
    transport + ClientSession context managers, satisfying anyio's rule
    that cancel scopes be exited in the same task that entered them.
    """

    session: ClientSession
    stop_event: asyncio.Event
    owner_task: asyncio.Task


class McpSessionManager:
    """Manages MCP client sessions per server URL.

    Sessions are lazily created on first use and kept alive for the
    duration of the request. Call ``close_all()`` in a finally block.

    Each session is held open by a dedicated "owner" asyncio task which
    enters the ``streamablehttp_client`` and ``ClientSession`` context
    managers, parks on a stop event, and then exits those contexts from
    within the same task on teardown. This is necessary because both of
    those context managers create ``anyio`` task groups whose cancel
    scopes are strictly bound to the task that entered them — closing
    them from a different task raises "Attempted to exit cancel scope in
    a different task than it was entered in". Tool calls issued from any
    task still work because ``ClientSession`` communicates through
    task-safe ``anyio`` memory object streams.
    """

    def __init__(
        self,
        logger: logging.Logger | None = None,
        token_context: TokenRefreshContext | None = None,
    ) -> None:
        self.logger = logger or logging.getLogger(__name__)
        self._token_context = token_context
        self._sessions: dict[str, _SessionEntry] = {}
        self._create_lock = asyncio.Lock()

    async def _refresh_token_if_needed(
        self, connection: McpConnectionInfo
    ) -> McpConnectionInfo:
        """Refresh an expired OAuth access token using the stored refresh token.

        Returns the original connection unchanged when refresh is not needed,
        not applicable (bearer tokens), or fails (graceful degradation).
        """
        ctx = self._token_context
        if ctx is None:
            return connection

        if connection.auth_type != McpAuthType.OAUTH:
            return connection

        if connection.expires_at is None:
            return connection

        now_ms = int(time.time() * 1000)
        if now_ms < connection.expires_at - _EXPIRY_BUFFER_MS:
            return connection

        # Token is expired (or about to expire) — attempt refresh
        if not connection.refresh_token or not connection.token_endpoint:
            self.logger.warning(
                f"OAuth token expired for {connection.server_url} but "
                "no refresh_token or token_endpoint available"
            )
            return connection

        try:
            return await self._do_refresh(connection, ctx)
        except Exception as e:
            self.logger.warning(
                f"OAuth token refresh failed for {connection.server_url}: {e}"
            )
            return connection

    async def _do_refresh(
        self, connection: McpConnectionInfo, ctx: TokenRefreshContext
    ) -> McpConnectionInfo:
        """Execute the OAuth refresh_token grant and persist the result."""
        form_data: dict[str, str] = {
            "grant_type": "refresh_token",
            "refresh_token": connection.refresh_token,
            "client_id": connection.client_id,
        }
        if connection.client_secret:
            form_data["client_secret"] = connection.client_secret
        if connection.resource:
            form_data["resource"] = connection.resource

        async with httpx.AsyncClient(timeout=_REFRESH_TIMEOUT_SECONDS) as client:
            resp = await client.post(
                connection.token_endpoint,
                data=form_data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            resp.raise_for_status()
            token_data = resp.json()

        new_access_token = token_data.get("access_token")
        if not new_access_token:
            self.logger.warning(
                f"Token endpoint did not return access_token for {connection.server_url}"
            )
            return connection

        new_refresh_token = token_data.get("refresh_token") or connection.refresh_token
        expires_in = token_data.get("expires_in")
        new_expires_at = (
            int(time.time() * 1000) + expires_in * 1000 if expires_in else None
        )

        # Build updated connection
        refreshed = McpConnectionInfo(
            server_url=connection.server_url,
            access_token=new_access_token,
            auth_type=connection.auth_type,
            refresh_token=new_refresh_token,
            token_endpoint=connection.token_endpoint,
            client_id=connection.client_id,
            client_secret=connection.client_secret,
            expires_at=new_expires_at,
            resource=connection.resource,
        )

        # Persist to etcd — read existing record, merge updates, write back
        url_hash = hash_server_url(connection.server_url)
        etcd_path = get_mcp_token_path(ctx.org_id, ctx.user_id, url_hash)

        # The encrypted store handles encryption of the whole record; store
        # access/refresh tokens as plaintext fields inside the dict.
        existing = await ctx.config_service.get_config(etcd_path) or {}
        existing.update(
            {
                "accessToken": new_access_token,
                "refreshToken": new_refresh_token or existing.get("refreshToken"),
                "expiresAt": new_expires_at,
                "updatedAt": int(time.time() * 1000),
            }
        )
        await ctx.config_service.set_config(etcd_path, existing)

        self.logger.info(f"OAuth token refreshed for {connection.server_url}")
        return refreshed

    async def _get_or_create_session(self, connection: McpConnectionInfo) -> ClientSession:
        """Return an existing session or spawn a new owner task for one."""
        url = connection.server_url

        # Fast path — no lock needed for cache hits.
        existing = self._sessions.get(url)
        if existing is not None:
            return existing.session

        async with self._create_lock:
            # Re-check after acquiring the lock (another caller may have
            # won the race for the same URL).
            existing = self._sessions.get(url)
            if existing is not None:
                return existing.session

            # Refresh expired OAuth tokens before establishing the session.
            connection = await self._refresh_token_if_needed(connection)

            ready_event = asyncio.Event()
            stop_event = asyncio.Event()
            state: dict[str, Any] = {"session": None, "error": None}

            async def _owner() -> None:
                """Enter transport + session contexts and park until stopped.

                Both ``__aenter__`` and ``__aexit__`` run inside this task,
                which is the invariant anyio's cancel scopes require.
                """
                try:
                    headers = {
                        "Authorization": f"Bearer {connection.access_token}"
                    }
                    async with streamablehttp_client(
                        url, headers=headers
                    ) as transport:
                        read_stream, write_stream, _ = transport
                        async with ClientSession(
                            read_stream, write_stream
                        ) as session:
                            await session.initialize()
                            state["session"] = session
                            ready_event.set()
                            # Park here until close_all() signals teardown.
                            await stop_event.wait()
                except BaseException as e:
                    # Record the failure (if any) and unblock the creator.
                    state["error"] = e
                    ready_event.set()
                    # Let CancelledError / KeyboardInterrupt continue to
                    # propagate so the task finishes in a cancelled state.
                    if not isinstance(e, Exception):
                        raise

            owner_task = asyncio.create_task(
                _owner(), name=f"mcp-owner:{url}"
            )

            try:
                await ready_event.wait()
            except BaseException:
                # The creator is being cancelled before the session was
                # ready — cancel the owner task so it can unwind its own
                # cancel scopes inside its own task context.
                owner_task.cancel()
                try:
                    await owner_task
                except BaseException:
                    pass
                raise

            if state["error"] is not None:
                # Wait for the owner task to finish its own cleanup before
                # surfacing the error.
                try:
                    await owner_task
                except BaseException:
                    pass
                raise state["error"]

            entry = _SessionEntry(
                session=state["session"],
                stop_event=stop_event,
                owner_task=owner_task,
            )
            self._sessions[url] = entry
            self.logger.info(f"MCP session established: {url}")
            return entry.session

    async def list_tools(self, connection: McpConnectionInfo) -> list[McpTool]:
        """List tools from an MCP server."""
        session = await self._get_or_create_session(connection)
        result = await session.list_tools()
        return list(result.tools)

    async def call_tool(
        self,
        connection: McpConnectionInfo,
        tool_name: str,
        arguments: dict[str, Any] | None = None,
    ) -> str:
        """Call a tool on an MCP server and return the result as a string."""
        session = await self._get_or_create_session(connection)
        from datetime import timedelta

        result: CallToolResult = await session.call_tool(
            tool_name,
            arguments or {},
            read_timeout_seconds=timedelta(seconds=MCP_TOOL_CALL_TIMEOUT_SECONDS),
        )

        if result.isError:
            error_text = _extract_text_content(result)
            return json.dumps({"status": "error", "message": error_text or "MCP tool returned an error"})

        return _extract_text_content(result) or ""

    async def close_all(self) -> None:
        """Signal each owner task to tear down and wait for completion.

        The owner task exits the transport and ``ClientSession`` contexts
        from inside its own task context, which is required for anyio's
        cancel scopes. This method only signals and awaits — it never
        enters or exits those contexts itself, so it is safe to call from
        any task (including a different task than the one that originally
        created the session).
        """
        entries = list(self._sessions.items())
        self._sessions.clear()
        for url, entry in entries:
            try:
                entry.stop_event.set()
                await entry.owner_task
                self.logger.debug(f"MCP session closed: {url}")
            except Exception as e:
                self.logger.warning(f"Error closing MCP session {url}: {e}")


def _extract_text_content(result: CallToolResult) -> str:
    """Extract text from a CallToolResult's content blocks."""
    parts: list[str] = []
    for block in result.content:
        if hasattr(block, "text"):
            parts.append(block.text)
        elif hasattr(block, "data"):
            parts.append(f"[binary data: {getattr(block, 'mimeType', 'unknown')}]")
        else:
            parts.append(str(block))
    return "\n".join(parts)
