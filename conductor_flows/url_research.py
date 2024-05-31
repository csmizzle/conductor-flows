from prefect import flow, task
import re
from typing import Union
import logging
from apis import ConductorApi
from utils import save_flow_result


logger = logging.getLogger(__name__)
conductor_api = ConductorApi()


@task(name="Collect Discord Messages")
def collect_discord_messages() -> Union[dict, None]:
    latest_messages = conductor_api.get_latest_messages()
    if latest_messages.ok:
        return latest_messages.json()
    else:
        logger.error(f"Failed to get latest messages: {latest_messages.status_code}")


@task(name="Extract HTML from Discord Messages")
def extract_html(message: dict):
    url_pattern = re.compile(
        r"http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"
    )
    urls = re.findall(url_pattern, message)
    return urls


@task(name="URL Summary Task")
def summarize_urls(urls: list[str]) -> Union[dict, None]:
    url_summary = conductor_api.post_summarize_urls(urls)
    if url_summary.ok:
        return url_summary.json()
    else:
        logger.error(f"Failed to summarize URL: {url_summary.status_code}")


@task(
    name="Wait for URL Summary Task",
    description="Polling Conductor Task API until job is complete",
)
def wait_for_url_summary(url_summary_response: dict) -> Union[str, None]:
    task_id = url_summary_response.get("task_id")
    logger.info(f"Waiting for task {task_id} to complete ...")
    task_complete = conductor_api.wait_for_collect_task(task_id)
    if task_complete:
        logger.info(f"Task {task_id} is complete, collecting summaries ...")
        return task_id


@task(name="Collect All URL Summaries")
def collect_all_url_summaries(task_id: str) -> Union[list[dict], None]:
    # collect all summaries with task_id from s3 bucket
    url_summaries = conductor_api.get_collect_task_status(task_id)
    if url_summaries.ok:
        logger.info(f"Collecting all URL summaries for task {task_id} ...")
        return url_summaries.json().get("url_summary")
    else:
        logger.error(f"Failed to get apify objects: {url_summaries.status_code}")


@task(name="Get Final Summary")
def submit_final_summary(summary_data: list[dict]) -> Union[dict, None]:
    contents = [entry["content"] for entry in summary_data]
    final_summary = conductor_api.post_chains_summarize(contents)
    if final_summary.ok:
        return final_summary.json()
    else:
        logger.error(f"Failed to summarize URL: {final_summary.status_code}")


@task(
    name="Wait for Final Summary Task",
    description="Polling Conductor Task API until job is complete",
)
def wait_for_final_summary(url_summary_response: dict) -> Union[str, None]:
    task_id = url_summary_response.get("task_id")
    logger.info(f"Waiting for task {task_id} to complete ...")
    task_complete = conductor_api.wait_for_chain_task(task_id)
    if task_complete:
        logger.info(f"Task {task_id} is complete, collecting summaries ...")
        return task_id


@task(name="Collect Final Summary")
def get_final_summary(task_id: str) -> Union[list[dict], None]:
    # collect all summaries with task_id from s3 bucket
    final_summary = conductor_api.get_chains_task(task_id)
    if final_summary.ok:
        logger.info(f"Collecting final summary for task {task_id} ...")
        return final_summary.json().get("summary")
    else:
        logger.error(
            f"Failed to get final summary objects: {final_summary.status_code}"
        )


@flow(
    name="Discord URL Research Flow",
    description="Extract URLs from Discord messages and summarize them",
)
def url_research_flow():
    urls = []
    messages = collect_discord_messages()
    if messages:
        for message in messages:
            urls.extend(extract_html(message["message"]))
    url_summaries_task = summarize_urls(urls[0:2])
    wait_for_url_summary_task = wait_for_url_summary(url_summaries_task)
    if wait_for_url_summary_task:
        summary_data = collect_all_url_summaries(wait_for_url_summary_task)
        submitted_final_summary = submit_final_summary(summary_data)
        wait_for_final_summary_task = wait_for_final_summary(submitted_final_summary)
        if wait_for_final_summary_task:
            final_summary = get_final_summary(wait_for_final_summary_task)
            save_flow_result(final_summary)
