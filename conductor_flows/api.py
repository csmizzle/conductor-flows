import os
import requests
from requests.models import Response
import polling2


class ConductorApi:
    """
    Conductor API Client
    """

    def __init__(self):
        self.conductor_url = os.getenv("CONDUCTOR_URL")
        self.conductor_username = os.getenv("CONDUCTOR_USERNAME")
        self.conductor_password = os.getenv("CONDUCTOR_PASSWORD")

    def get_latest_messages(self):
        return requests.get(
            url=f"{self.conductor_url}/buckets/object/latest/",
            params={"bucket_name": "discord-bucket-dev"},
            auth=(self.conductor_username, self.conductor_password),
        )

    def post_summarize_urls(self, urls):
        return requests.post(
            url=f"{self.conductor_url}/collect/url/summarize/",
            json={"urls": urls},
            auth=(self.conductor_username, self.conductor_password),
        )

    def get_collect_task_status(self, task_id):
        return requests.get(
            url=f"{self.conductor_url}/collect/tasks/{task_id}/",
            auth=(self.conductor_username, self.conductor_password),
        )

    def wait_for_collect_task(self, task_id):
        return polling2.poll(
            lambda: self.get_collect_task_status(task_id).json().get("status") == "C",
            step=5,
            timeout=60 * 10,
        )

    def post_chains_summarize(self, contents):
        return requests.post(
            url=f"{self.conductor_url}/chains/summarize/",
            json={"content": contents},
            auth=(self.conductor_username, self.conductor_password),
        )

    def get_chains_task(self, task_id):
        return requests.get(
            url=f"{self.conductor_url}/chains/tasks/{task_id}/",
            auth=(self.conductor_username, self.conductor_password),
        )

    def wait_for_chain_task(self, task_id):
        return polling2.poll(
            lambda: self.get_chains_task(task_id).json().get("status") == "C",
            step=5,
            timeout=60 * 10,
        )

    def save_result(self, result, flow_trace: int):
        return requests.post(
            url=f"{self.conductor_url}/results/",
            json={
                "flow_trace": flow_trace,
                "results": result,
            },
            auth=(self.conductor_username, self.conductor_password),
        )

    def post_apollo_context(
        self,
        person_titles: list[str],
        person_locations: list[str],
        flow_trace: int = None,
    ) -> Response:
        """
        Get Apollo context for a person
        """
        return requests.post(
            url=f"{self.conductor_url}/chains/apollo/context/",
            json={
                "person_titles": person_titles,
                "person_locations": person_locations,
                "flow_trace": flow_trace,
            },
            auth=(self.conductor_username, self.conductor_password),
        )

    def post_apollo_input(self, query: str, flow_trace: int = None) -> Response:
        """
        Get Apollo input for a query
        """
        return requests.post(
            url=f"{self.conductor_url}/chains/apollo/input/",
            json={"query": query, "flow_trace": flow_trace},
            auth=(self.conductor_username, self.conductor_password),
        )

    def post_email_from_context(
        self, context: str, tone: str, sign_off: str, flow_trace: int = None
    ) -> Response:
        """
        Create an email from context and tone
        """
        return requests.post(
            url=f"{self.conductor_url}/chains/email/context/",
            json={
                "context": context,
                "tone": tone,
                "sign_off": sign_off,
                "flow_trace": flow_trace,
            },
            auth=(self.conductor_username, self.conductor_password),
        )

    def create_flow_trace(
        self,
        prefect_id: str,
        prefect_deployment_id: str,
        prefect_name: str,
        prefect_parameters: dict = None,
    ) -> Response:
        return requests.post(
            url=f"{self.conductor_url}/flows/",
            json={
                "prefect_flow_id": prefect_id,
                "prefect_deployment_id": prefect_deployment_id,
                "prefect_name": prefect_name,
                "prefect_parameters": prefect_parameters,
            },
            auth=(self.conductor_username, self.conductor_password),
        )
