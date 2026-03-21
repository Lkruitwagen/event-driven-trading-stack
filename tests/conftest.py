import pytest
from fastapi.testclient import TestClient

from edts.cli import starter, stopper
from edts.pubsub import app as pubsub_app

DATABUS_PORT = 8000
DECISIONBUS_PORT = 8001
TRADER_PORT = 8002


@pytest.fixture
def pubsub_client():
    with TestClient(pubsub_app) as c:
        yield c


@pytest.fixture(scope="module")
def stack(tmp_path_factory):
    """Start the full stack and yield service info, then tear down."""
    log_path = tmp_path_factory.mktemp("logs") / "stack.log"
    log = open(log_path, "w")

    processes = {}
    try:
        p_databus = starter("edts.pubsub:app", DATABUS_PORT, log, health_check=True)
        processes["databus"] = {"proc": p_databus, "url": f"http://localhost:{DATABUS_PORT}"}

        p_decisionbus = starter("edts.pubsub:app", DECISIONBUS_PORT, log, health_check=True)
        processes["decisionbus"] = {
            "proc": p_decisionbus,
            "url": f"http://localhost:{DECISIONBUS_PORT}",
        }

        p_trader = starter("edts.trader:app", TRADER_PORT, log, health_check=True)
        processes["trader"] = {"proc": p_trader, "url": f"http://localhost:{TRADER_PORT}"}

        yield processes

    finally:
        for name in ["trader", "decisionbus", "databus"]:
            info = processes.get(name)
            if info:
                try:
                    stopper(info["url"], info["proc"].pid, force=True)
                except Exception:
                    pass
        log.close()
