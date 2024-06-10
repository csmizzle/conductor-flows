"""
Deploy flows
"""
from prefect import serve
from test_flow import test_flow
from conductor_flows.discord_url_research import url_research_flow
from market_research import market_research_flow


if __name__ == "__main__":
    test_flow_deployment = test_flow.to_deployment(
        name="Test Flow",
        description="Test flow",
    )
    url_deployment = url_research_flow.to_deployment(
        name="Discord URL Research Flow",
        description="Extract URLs from Discord messages and summarize them",
    )
    market_deployment = market_research_flow.to_deployment(
        name="Market Research Flow",
        description="Create an email from a query",
    )
    serve(test_flow_deployment, url_deployment, market_deployment)
