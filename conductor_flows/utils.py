"""
Utils for working with conductor flows
"""
from typing import Any
import logging
from api import ConductorApi

logger = logging.getLogger(__name__)


def save_flow_result(api: ConductorApi, flow_trace: int, result: Any) -> None:
    """
    Use flow context to store results in conductor database
    """
    response = api.save_result(flow_trace=flow_trace, result=result)
    if response.ok:
        logger.info(f"Saved flow result: {response.status_code}")
    else:
        logger.error(f"Failed to save flow result: {response.status_code}")
    return response
