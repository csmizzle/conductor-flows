"""
Flow for market research
"""
from prefect import flow, task
from typing import Union
import logging
from apis import ConductorApi
from utils import save_flow_result


logger = logging.getLogger(__name__)
conductor_api = ConductorApi()


@task(name="Apollo Input Creation", description="Create Apollo input for a person")
def create_apollo_input(query: str) -> Union[dict, None]:
    apollo_input = conductor_api.post_apollo_input(query)
    if apollo_input.ok:
        return apollo_input.json()
    else:
        logger.error(f"Failed to create Apollo input: {apollo_input.status_code}")


@task(name="Apollo Context Creation", description="Create Apollo context for a person")
def create_apollo_context(
    person_titles: list[str], person_locations: list[str]
) -> Union[dict, None]:
    apollo_context = conductor_api.post_apollo_context(person_titles, person_locations)
    if apollo_context.ok:
        return apollo_context.json()
    else:
        logger.error(f"Failed to create Apollo context: {apollo_context.status_code}")


@task(name="Email Creation", description="Create an email from context and tone")
def create_email_from_context(
    context: str, tone: str, sign_off: str
) -> Union[dict, None]:
    email = conductor_api.post_email_from_context(context, tone, sign_off)
    if email.ok:
        return email.json()
    else:
        logger.error(f"Failed to create email: {email.status_code}")


@flow(name="Market Research Flow")
def market_research_flow(query: str) -> None:
    """
    Flow for market research
    """
    apollo_input = create_apollo_input(query)
    apollo_context = create_apollo_context(
        apollo_input.get("person_titles"), apollo_input.get("person_locations")
    )
    email = create_email_from_context(
        apollo_context.get("context"),
        apollo_context.get("tone"),
        apollo_context.get("sign_off"),
    )
    save_flow_result(api=conductor_api, result={"email": email})
