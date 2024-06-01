"""
Models for Prefect flows
"""
from pydantic import BaseModel


class MarketResearchFlow(BaseModel):
    """
    Query parameters for Market Research flow
    """

    query: str
