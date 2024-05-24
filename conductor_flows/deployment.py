"""
Deploy flows
"""
from url_research import url_research_flow


if __name__ == "__main__":
    url_research_flow.serve(
        name="Discord URL Research Flow",
        description="Extract URLs from Discord messages and summarize them"
    )
