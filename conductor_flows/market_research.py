"""
Flow for market research
"""
from prefect import flow, task, get_run_logger
from typing import Union
from api import ConductorApi
from utils import save_flow_result


conductor_api = ConductorApi()


@task(name="Apollo Input Creation", description="Create Apollo input for a person")
def create_apollo_input(query: str, flow_trace: int) -> Union[dict, None]:
    logger = get_run_logger()
    apollo_input = conductor_api.post_apollo_input(query=query, flow_trace=flow_trace)
    if apollo_input.ok:
        logger.info("Apollo input created")
        return apollo_input.json()
    else:
        logger.error(f"Failed to create Apollo input: {apollo_input.status_code}")


@task(name="Apollo Context Creation", description="Create Apollo context for a person")
def create_apollo_context(
    person_titles: list[str], person_locations: list[str], flow_trace: int
) -> Union[dict, None]:
    logger = get_run_logger()
    apollo_context = conductor_api.post_apollo_context(
        person_titles=person_titles,
        person_locations=person_locations,
        flow_trace=flow_trace,
    )
    if apollo_context.ok:
        logger.info("Apollo context created")
        return apollo_context.json()
    else:
        logger.error(f"Failed to create Apollo context: {apollo_context.status_code}")


@task(name="Email Creation", description="Create an email from context and tone")
def create_email_from_context(
    context: str, tone: str, sign_off: str, flow_trace: int
) -> Union[dict, None]:
    logger = get_run_logger()
    email = conductor_api.post_email_from_context(
        context=context,
        tone=tone,
        sign_off=sign_off,
        flow_trace=flow_trace,
    )
    if email.ok:
        logger.info("Email created")
        return email.json()
    else:
        logger.error(f"Failed to create email: {email.status_code}")


@flow(name="Market Research Flow")
def market_research_flow(flow_trace: int, query: str) -> None:
    """
    Flow for market research
    """
    print("Flow trace:", flow_trace)
    logger = get_run_logger()
    apollo_input = create_apollo_input(query=query, flow_trace=flow_trace)
    apollo_context = create_apollo_context(
        person_titles=apollo_input["output"].get("person_titles"),
        person_locations=apollo_input["output"].get("person_locations"),
        flow_trace=flow_trace,
    )
    email = create_email_from_context(
        context=apollo_context.get("output"),
        tone="formal",
        sign_off="Best, Research Team",
        flow_trace=flow_trace,
    )
    result = save_flow_result(
        api=conductor_api,
        flow_trace=flow_trace,
        result={"email": email["output"]["text"]},
    )
    if result.ok:
        logger.info("Market research flow completed")
    else:
        logger.error(f"Failed to save flow result: {result.status_code}")
