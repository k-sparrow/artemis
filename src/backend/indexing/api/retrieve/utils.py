# -------------------------------------
# Copyright (c) 2026, Dror Kabely
# -------------------------------------
#
from __future__ import annotations

from typing import Any, Dict

from fastapi import HTTPException, Request


__all__ = [
    "pre_req_config_modifier",
]


async def pre_req_config_modifier(
    config: Dict[str, Any], request: Request
) -> Dict[str, Any]:
    if "configurable" not in config:
        config["configurable"] = {}

    req_json = await request.json()
    configurable_from_request = req_json.get("config", {}).get("configurable", {})
    config["configurable"].update(configurable_from_request)

    if not config["configurable"].get("namespace_id"):
        raise HTTPException(
            status_code=400,
            detail="namespace_id is required in config.configurable",
        )

    return config
