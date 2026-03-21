import subprocess
from time import sleep

import pytest
import requests

from edts.cli import stopper


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
