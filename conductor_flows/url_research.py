from prefect import flow, task
from typing import Union
import logging
from api import ConductorApi


logger = logging.getLogger(__name__)
conductor_api = ConductorApi()


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


@task(name="Get Apollo People Context")
def get_apollo_people_context(company_domains: list[str]) -> dict:
    domain_contexts = {}
    for domain in company_domains:
        people_context = conductor_api.post_apollo_people_context([domain])
        if people_context.ok:
            logger.info("Returning Apollo people context ...")
            domain_contexts[domain] = people_context.json().get("output")
        else:
            logger.error(
                f"Failed to get final summary objects: {people_context.status_code}"
            )
    return domain_contexts


@flow(
    name="URL Research Flow",
    description="Takes a list of URLs from and summarizes them",
)
def url_research_flow(flow_trace: int, urls: list[str]):
    url_summaries_task = summarize_urls(urls)
    # people_contexts = get_apollo_people_context(urls)
    wait_for_url_summary_task = wait_for_url_summary(url_summaries_task)
    if wait_for_url_summary_task:
        summary_data = collect_all_url_summaries(wait_for_url_summary_task)
        submitted_final_summary = submit_final_summary(summary_data)
        wait_for_final_summary_task = wait_for_final_summary(submitted_final_summary)
        if wait_for_final_summary_task:
            final_summary = get_final_summary(wait_for_final_summary_task)
            # generate report
            paragraphs = [
                {"title": "Executive Summary", "content": final_summary[0]["summary"]},
            ]
            for entry in summary_data:
                paragraphs.append(
                    {
                        "title": "Summary of " + entry["url"],
                        "content": entry["summary"],
                    }
                )
            report = {
                "title": "Syrinx URL Research Report",
                "description": "Summary of URLs extracted from Discord messages",
                "paragraphs": paragraphs,
            }
            conductor_api.post_report(report)
