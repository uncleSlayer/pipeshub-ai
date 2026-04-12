"""MCP tool provider — discovers MCP tools and converts them to LangChain StructuredTools."""

import asyncio
import json
import logging
import re
from typing import Any, Optional
from urllib.parse import urlparse

from langchain_core.tools import StructuredTool
from mcp.types import Tool as McpTool
from pydantic import BaseModel, Field, create_model

from app.agents.tools.mcp.constants import MCP_DISCOVERY_TIMEOUT_SECONDS
from app.agents.tools.mcp.session_manager import McpSessionManager
from app.agents.tools.mcp.types import McpConnectionInfo

# JSON Schema type → Python type mapping
_JSON_SCHEMA_TYPE_MAP: dict[str, type] = {
    "string": str,
    "integer": int,
    "number": float,
    "boolean": bool,
    "array": list,
    "object": dict,
}


class McpToolProvider:
    """Discovers tools from MCP servers and wraps them as LangChain StructuredTools."""

    def __init__(
        self,
        connections: list[McpConnectionInfo],
        session_manager: McpSessionManager,
        logger: logging.Logger | None = None,
    ) -> None:
        self.connections = connections
        self.session_manager = session_manager
        self.logger = logger or logging.getLogger(__name__)

    async def discover_all_tools(self) -> list[StructuredTool]:
        """Discover tools from all MCP servers in parallel (soft-fail per server)."""
        if not self.connections:
            return []

        tasks = [
            asyncio.wait_for(
                self._discover_server_tools(conn),
                timeout=MCP_DISCOVERY_TIMEOUT_SECONDS,
            )
            for conn in self.connections
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        tools: list[StructuredTool] = []
        for conn, result in zip(self.connections, results):
            if isinstance(result, Exception):
                self.logger.warning(
                    f"MCP discovery failed for {conn.server_url} (soft-fail): {result}"
                )
            else:
                tools.extend(result)

        return tools

    async def _discover_server_tools(
        self, connection: McpConnectionInfo
    ) -> list[StructuredTool]:
        """Discover and convert tools from a single MCP server."""
        mcp_tools = await self.session_manager.list_tools(connection)
        self.logger.info(
            f"Discovered {len(mcp_tools)} tools from {connection.server_url}"
        )

        structured_tools: list[StructuredTool] = []
        for mcp_tool in mcp_tools:
            try:
                tool = self._mcp_tool_to_structured_tool(connection, mcp_tool)
                structured_tools.append(tool)
            except Exception as e:
                self.logger.warning(
                    f"Failed to convert MCP tool '{mcp_tool.name}' "
                    f"from {connection.server_url}: {e}"
                )
        return structured_tools

    def _mcp_tool_to_structured_tool(
        self, connection: McpConnectionInfo, mcp_tool: McpTool
    ) -> StructuredTool:
        """Convert a single MCP tool definition to a LangChain StructuredTool."""
        tool_name = _make_tool_name(connection.server_url, mcp_tool.name)
        description = mcp_tool.description or f"MCP tool: {mcp_tool.name}"

        # Pass the MCP server's raw JSON Schema straight through to LangChain.
        # langchain_core>=0.3 accepts args_schema as a dict (JSON Schema) and
        # forwards it verbatim to the LLM. The previous Pydantic round-trip
        # via _build_args_schema flattened oneOf/anyOf/nested-object fields to
        # `str` (e.g. Notion's `parent` discriminated union), causing the LLM
        # to send strings where the MCP server expects objects. Pass-through
        # preserves unions, enums, nested properties, descriptions, etc.
        args_schema = mcp_tool.inputSchema or {"type": "object", "properties": {}}

        # Create the async closure that actually calls the MCP tool
        original_name = mcp_tool.name
        session_mgr = self.session_manager
        conn = connection
        tool_logger = self.logger

        async def _call_mcp_tool(**kwargs: Any) -> dict:
            try:
                # Strip None values — many MCP servers (e.g. Notion) reject
                # nulls for optional fields and expect them to be omitted.
                kwargs = {k: v for k, v in kwargs.items() if v is not None}
                tool_logger.info(
                    f"MCP tool call: '{original_name}' on {conn.server_url} args={kwargs}"
                )
                result = await session_mgr.call_tool(conn, original_name, kwargs)
                tool_logger.info(
                    f"MCP tool '{original_name}' result: {result}"
                )
                return {
                    "ok": True,
                    "result_type": "mcp",
                    "result": result,
                    "tool": original_name,
                    "server": conn.server_url,
                }
            except Exception as e:
                tool_logger.error(f"MCP tool '{original_name}' execution failed: {e}")
                return {
                    "ok": False,
                    "result_type": "mcp",
                    "error": str(e),
                    "tool": original_name,
                    "server": conn.server_url,
                }

        return StructuredTool.from_function(
            func=_call_mcp_tool,
            name=tool_name,
            description=description,
            args_schema=args_schema,
            coroutine=_call_mcp_tool,
        )


def _make_tool_name(server_url: str, tool_name: str) -> str:
    """Generate a unique, LLM-safe tool name.

    Format: mcp_{sanitized_host}_{tool_name}
    LLM APIs require ^[a-zA-Z0-9_-]+$ (no dots, slashes, etc.).
    """
    parsed = urlparse(server_url)
    host = parsed.hostname or "unknown"
    # Replace non-alphanumeric chars with underscore, collapse multiples
    sanitized_host = re.sub(r"[^a-zA-Z0-9]", "_", host)
    sanitized_host = re.sub(r"_+", "_", sanitized_host).strip("_")
    sanitized_tool = re.sub(r"[^a-zA-Z0-9_-]", "_", tool_name)
    sanitized_tool = re.sub(r"_+", "_", sanitized_tool).strip("_")
    return f"mcp_{sanitized_host}_{sanitized_tool}"


def _build_args_schema(
    tool_name: str, input_schema: dict[str, Any]
) -> type[BaseModel]:
    """Convert a JSON Schema ``inputSchema`` to a Pydantic model."""
    properties = input_schema.get("properties", {})
    required_fields = set(input_schema.get("required", []))

    if not properties:
        # No parameters — create an empty model
        return create_model(f"{tool_name}_Args")

    fields: dict[str, Any] = {}
    for name, prop in properties.items():
        py_type = _json_schema_type_map(prop)
        description = prop.get("description", "")

        if name in required_fields:
            fields[name] = (py_type, Field(description=description))
        else:
            default = prop.get("default")
            fields[name] = (
                Optional[py_type],
                Field(default=default, description=description),
            )

    return create_model(f"{tool_name}_Args", **fields)


def _json_schema_type_map(prop: dict[str, Any]) -> type:
    """Map a JSON Schema property to a Python type."""
    schema_type = prop.get("type", "string")
    if isinstance(schema_type, list):
        # Union types — take the first non-null type
        schema_type = next((t for t in schema_type if t != "null"), "string")
    return _JSON_SCHEMA_TYPE_MAP.get(schema_type, str)
