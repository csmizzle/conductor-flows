from prefect import flow, task
import re
from typing import Union
import requests
import os
import polling2
import logging


logger = logging.getLogger(__name__)
CONDUCTOR_URL = os.getenv("CONDUCTOR_URL")
CONDUCTOR_USERNAME = os.getenv("CONDUCTOR_USERNAME")
CONDUCTOR_PASSWORD = os.getenv("CONDUCTOR_PASSWORD")


@task(name="Collect Discord Messages")
def collect_discord_messages() -> Union[dict, None]:
    latest_messages = requests.get(
        url=f"{CONDUCTOR_URL}/buckets/object/latest/",
        params={"bucket_name": "discord-bucket-dev"},
        auth=(CONDUCTOR_USERNAME, CONDUCTOR_PASSWORD)
    )
    if latest_messages.ok:
        return latest_messages.json()
    else:
        logger.error(f"Failed to get latest messages: {latest_messages.status_code}")


@task(name="Extract HTML from Discord Messages")
def extract_html(message: dict):
    url_pattern = re.compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
    urls = re.findall(url_pattern, message)
    return urls


@task(name="URL Summary Task")
def summarize_urls(urls: list[str]) -> Union[dict, None]:
    url_summary = requests.post(
        url=f"{CONDUCTOR_URL}/collect/url/summarize/",
        json={"urls": urls},
        auth=(CONDUCTOR_USERNAME, CONDUCTOR_PASSWORD)
    )
    if url_summary.ok:
        return url_summary.json()
    else:
        logger.error(f"Failed to summarize URL: {url_summary.status_code}")


@task(
    name="Wait for URL Summary Task",
    description="Polling Conductor Task API until job is complete"
)
def wait_for_url_summary(url_summary_response: dict) -> Union[str, None]:
    task_id = url_summary_response.get("task_id")
    logger.info(f"Waiting for task {task_id} to complete ...")
    task_complete = polling2.poll(
        lambda: requests.get(
            url=f"{CONDUCTOR_URL}/collect/tasks/{task_id}/",
            auth=(CONDUCTOR_USERNAME, CONDUCTOR_PASSWORD)
        ).json().get("status") == "C",
        step=5,
        timeout=60*10
    )
    if task_complete:
        logger.info(f"Task {task_id} is complete, collecting summaries ...")
        return task_id


@task(name="Collect All URL Summaries")
def collect_all_url_summaries(task_id: str) -> Union[list[dict], None]:
    # collect all summaries with task_id from s3 bucket
    url_summaries = requests.get(
        url=f"{CONDUCTOR_URL}/collect/tasks/{task_id}/",
        auth=(CONDUCTOR_USERNAME, CONDUCTOR_PASSWORD)
    )
    if url_summaries.ok:
        logger.info(f"Collecting all URL summaries for task {task_id} ...")
        return url_summaries.json().get("url_summary")
    else:
        logger.error(f"Failed to get apify objects: {url_summaries.status_code}")


@task(name="Get Final Summary")
def submit_final_summary(summary_data: list[dict]) -> Union[dict, None]:
    contents = [entry['content'] for entry in summary_data]
    final_summary = requests.post(
        url=f"{CONDUCTOR_URL}/chains/summarize/",
        json={"content": contents},
        auth=(CONDUCTOR_USERNAME, CONDUCTOR_PASSWORD)
    )
    if final_summary.ok:
        return final_summary.json()
    else:
        logger.error(f"Failed to summarize URL: {final_summary.status_code}")


@task(
    name="Wait for Final Summary Task",
    description="Polling Conductor Task API until job is complete"
)
def wait_for_final_summary(url_summary_response: dict) -> Union[str, None]:
    task_id = url_summary_response.get("task_id")
    logger.info(f"Waiting for task {task_id} to complete ...")
    task_complete = polling2.poll(
        lambda: requests.get(
            url=f"{CONDUCTOR_URL}/chains/tasks/{task_id}/",
            auth=(CONDUCTOR_USERNAME, CONDUCTOR_PASSWORD)
        ).json().get("status") == "C",
        step=5,
        timeout=60*10
    )
    if task_complete:
        logger.info(f"Task {task_id} is complete, collecting summaries ...")
        return task_id


@task(name="Collect Final Summary")
def get_final_summary(task_id: str) -> Union[list[dict], None]:
    # collect all summaries with task_id from s3 bucket
    final_summary = requests.get(
        url=f"{CONDUCTOR_URL}/chains/tasks/{task_id}/",
        auth=(CONDUCTOR_USERNAME, CONDUCTOR_PASSWORD)
    )
    if final_summary.ok:
        logger.info(f"Collecting final summary for task {task_id} ...")
        return final_summary.json().get("summary")
    else:
        logger.error(f"Failed to get final summary objects: {final_summary.status_code}")


@flow(
    name="Discord URL Research Flow",
    description="Extract URLs from Discord messages and summarize them"
)
def url_research_flow():
    urls = []
    messages = collect_discord_messages()
    if messages:
        for message in messages:
            urls.extend(extract_html(message['message']))
    url_summaries_task = summarize_urls(urls)
    wait_for_url_summary_task = wait_for_url_summary(url_summaries_task)
    if wait_for_url_summary_task:
        summary_data = collect_all_url_summaries(wait_for_url_summary_task)
        submitted_final_summary = submit_final_summary(summary_data)
        wait_for_final_summary_task = wait_for_final_summary(submitted_final_summary)
        if wait_for_final_summary_task:
            final_summary = get_final_summary(wait_for_final_summary_task)
            print(final_summary)


if __name__ == "__main__":
    url_research_flow()
