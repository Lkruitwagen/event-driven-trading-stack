import os

import pytest
import requests

from edts.cli import starter, stopper

DATABUS_PORT = 8000
GENERATOR_PORT = 8100
TOPIC = "prices"


@pytest.fixture(scope="module")
def generator(stack, tmp_path_factory):
    """Start the random_walk generator pointed at the DataBus, then tear down."""
    log_path = tmp_path_factory.mktemp("logs") / "generator.log"
    log = open(log_path, "w")

    databus_url = stack["databus"]["url"]
    env = {
        **os.environ,
        "TOPIC_URL": f"{databus_url}/publish/{TOPIC}",
    }
    p = starter(
        "edts.generators.random_walk:app",
        GENERATOR_PORT,
        log,
        health_check=True,
        env=env,
    )
    yield {"proc": p, "url": f"http://localhost:{GENERATOR_PORT}"}

    try:
        stopper(f"http://localhost:{GENERATOR_PORT}", p.pid, force=True)
    except Exception:
        pass
    log.close()


def test_generator_healthy(generator):
    r = requests.get(f"{generator['url']}/health")
    assert r.status_code == 200
    assert r.json() == {"status": "healthy"}


def test_topic_registered_on_databus(stack, generator):
    databus_url = stack["databus"]["url"]
    r = requests.get(f"{databus_url}/status")
    assert r.status_code == 200
    status = r.json()
    assert TOPIC in status["topics"], f"Topic '{TOPIC}' not found in DataBus status: {status}"
