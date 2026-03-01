import pytest


# Configure pytest-asyncio to use auto mode
pytest_plugins = ["pytest_asyncio"]


def pytest_configure(config):
    """Configure pytest-asyncio mode."""
    config.addinivalue_line("markers", "asyncio: mark test as async")
    config.addinivalue_line(
        "markers",
        "integration: mark test as an integration test that requires Docker",
    )
