import pytest


# Configure pytest-asyncio to use auto mode
pytest_plugins = ["pytest_asyncio"]


def pytest_addoption(parser):
    parser.addoption(
        "--e2e",
        action="store_true",
        default=False,
        help="Run Tier 4 end-to-end tests (requires all service images to be loaded)",
    )


def pytest_configure(config):
    """Configure pytest-asyncio mode."""
    config.addinivalue_line("markers", "asyncio: mark test as async")
    config.addinivalue_line(
        "markers",
        "integration: mark test as an integration test that requires Docker",
    )
    config.addinivalue_line(
        "markers",
        "e2e: Tier 4 end-to-end test — full stack, no stubs (pass --e2e to run)",
    )


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--e2e"):
        skip = pytest.mark.skip(reason="pass --e2e to run end-to-end tests")
        for item in items:
            if "e2e" in item.keywords:
                item.add_marker(skip)
