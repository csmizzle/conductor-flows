import os
import requests
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
