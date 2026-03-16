# Set required env vars before any module-level settings objects are instantiated.
# These are dummy values — all infrastructure dependencies are overridden via
# FastAPI's dependency_overrides in the test fixtures.
import os

os.environ.setdefault("DOCLING_SERVE_URI", "http://test-docling:5001")
