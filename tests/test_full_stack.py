import subprocess
from time import sleep

import pytest
import requests

from edts.cli import starter, stopper

DATABUS_PORT = 8000
DECISIONBUS_PORT = 8001
TRADER_PORT = 8002


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


def test_services_healthy(stack):
    for name, info in stack.items():
        r = requests.get(f"{info['url']}/health")
        assert r.status_code == 200, f"{name} health check failed"
        assert r.json() == {"status": "healthy"}, f"{name} unexpected health response"


def test_trader_executes_ticks(stack):
    """Let the trader run for a couple of ticks (5s each) and confirm it stays healthy."""
    sleep(12)  # covers at least 2 trader ticks
    r = requests.get(f"{stack['trader']['url']}/health")
    assert r.status_code == 200


def test_stack_down(stack):
    """Graceful shutdown of all services."""
    for name in ["trader", "decisionbus", "databus"]:
        info = stack[name]
        existed = stopper(info["url"], info["proc"].pid, force=False)
        assert existed, f"{name} was not running when shutdown was requested"

    for name, info in stack.items():
        try:
            info["proc"].wait(timeout=10)
        except subprocess.TimeoutExpired:
            pytest.fail(f"{name} process still running after 10s shutdown timeout")
