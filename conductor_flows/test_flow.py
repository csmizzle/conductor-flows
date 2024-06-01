from prefect import flow


@flow(name="Test Flow")
def test_flow() -> str:
    """
    Test flow
    """
    return "Test Flow working"
