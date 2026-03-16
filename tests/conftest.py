import pytest
from fastapi.testclient import TestClient

from edts.pubsub import app as pubsub_app


@pytest.fixture
def pubsub_client():
    with TestClient(pubsub_app) as c:
        yield c
