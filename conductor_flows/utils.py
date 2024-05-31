"""
Utils for working with conductor flows
"""
import prefect
from typing import Any
import prefect.context
import logging
from apis import ConductorApi

logger = logging.getLogger(__name__)


def save_flow_result(api: ConductorApi, result: Any) -> None:
    """
    Use flow context to store results in conductor database
    """
    context_data = prefect.context.get_run_context().flow_run.dict()
    id = str(context_data["id"])
    flow_id = str(context_data["flow_id"])
    deployment_id = str(context_data["deployment_id"])
    response = api.save_result(result, id, flow_id, deployment_id)
    if response.ok:
        logger.info(f"Saved flow result: {response.status_code}")
    else:
        logger.error(f"Failed to save flow result: {response.status_code}")
