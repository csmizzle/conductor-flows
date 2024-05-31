"""
Utils for working with conductor flows
"""
import prefect
from typing import Any
import prefect.context
import requests
import os


def save_flow_result(result: Any) -> None:
    """
    Use flow context to store results in conductor database
    """
    context_data = prefect.context.get_run_context().flow_run.json()
    print(context_data)
    response = requests.post(
        url=f"{os.getenv('CONDUCTOR_URL')}/flows/results/",
        json={
            "context": context_data,
            "result": result,
        },
    )
    if response.ok:
        print("Flow result saved successfully")
        print(response.json())
    else:
        print(f"Failed to save flow result: {response.status_code}")
