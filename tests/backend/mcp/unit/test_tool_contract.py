# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
"""Contract / snapshot test for MCP tool schemas.

Calls mcp.list_tools() and compares the result against the checked-in
snapshot at tool_schema_snapshot.json.  The snapshot captures:
  - tool names (which tools exist)
  - inputSchema (parameter names, types, defaults, constraints)
  - annotations (readOnlyHint, destructiveHint, idempotentHint, openWorldHint)

Docstrings are intentionally excluded — they may change for readability
without constituting a breaking interface change.

To regenerate the snapshot after an intentional schema change:

    UPDATE_SNAPSHOTS=1 pytest tests/backend/mcp/unit/test_tool_contract.py \
        --no-header -q

(Run outside Bazel so the test can write back to the source tree.)
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from src.backend.mcp.api.tools import mcp

_SNAPSHOT_PATH = Path(__file__).parent / "tool_schema_snapshot.json"


def _dump_tools(tools) -> list[dict]:
    result = []
    for tool in sorted(tools, key=lambda t: t.name):
        result.append(
            {
                "name": tool.name,
                "inputSchema": tool.inputSchema,
                "annotations": (
                    tool.annotations.model_dump() if tool.annotations else None
                ),
            }
        )
    return result


@pytest.mark.asyncio
async def test_tool_schema_contract():
    tools = await mcp.list_tools()
    current = _dump_tools(tools)

    if os.environ.get("UPDATE_SNAPSHOTS"):
        _SNAPSHOT_PATH.write_text(json.dumps(current, indent=2) + "\n")
        return

    assert _SNAPSHOT_PATH.exists(), (
        f"Snapshot not found at {_SNAPSHOT_PATH}. "
        "Run with UPDATE_SNAPSHOTS=1 to generate it."
    )

    snapshot = json.loads(_SNAPSHOT_PATH.read_text())
    assert current == snapshot, (
        "MCP tool schema has changed. If this is intentional, regenerate the snapshot:\n\n"
        "    UPDATE_SNAPSHOTS=1 pytest tests/backend/mcp/unit/test_tool_contract.py\n\n"
        f"Got:\n{json.dumps(current, indent=2)}\n\n"
        f"Expected:\n{json.dumps(snapshot, indent=2)}"
    )
