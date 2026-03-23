import os
import subprocess
from io import TextIOWrapper
from time import sleep

import requests
import typer
import yaml

STARTUP_TIMEOUT = 10  # seconds


app = typer.Typer(help="Event-driven trading stack CLI.")
generator_app = typer.Typer(help="Manage generators.")
strategy_app = typer.Typer(help="Manage strategies.")
app.add_typer(generator_app, name="generator")
app.add_typer(strategy_app, name="strategy")

GENERATOR_MODULES = {
    "random_walk": "edts.generators.random_walk:app",
}

STRATEGY_MODULES = {
    "mean_reversion": "edts.strategies.mean_reversion:app",
}


def starter(
    module: str,
    port: int,
    output: int | TextIOWrapper,
    health_check: bool = True,
    env: dict | None = None,
):
    p = subprocess.Popen(
        ["uvicorn", f"{module}", "--port", str(port)],
        stdout=output,
        stderr=output,
        env=env,
    )
    if health_check:
        accumulated_time = 0
        while True and accumulated_time < STARTUP_TIMEOUT:
            try:
                r = requests.get(f"http://localhost:{port}/health")
                if r.status_code == 200:
                    return p
            except requests.exceptions.ConnectionError:
                pass
            sleep(2)
            accumulated_time += 2

        raise ValueError(
            f"Service {module} on port {port} failed to start within {STARTUP_TIMEOUT} seconds."
        )

    return p


def stopper(url: str, pid: int, force: bool = False):
    # gracefully stop the service, return whether the service existed or not
    try:
        r = requests.post(f"{url}/shutdown")
        r.raise_for_status()
        return True
    except requests.exceptions.ConnectionError as e:
        # connection failed — service may already be offline; verify via PID
        result = subprocess.run(["kill", "-0", str(pid)], capture_output=True)
        if result.returncode != 0:
            return False  # process is gone, nothing to do
        raise e
    except requests.RequestException as e:
        if force:
            subprocess.run(["kill", str(pid)], check=True)
            return True
        else:
            raise e


def stream_logs():
    typer.echo("Streaming logs from stack.log (Ctrl+C to stop)...")
    try:
        with open("stack.log", "r") as f:
            f.seek(0)
            while True:
                line = f.readline()
                if line:
                    typer.echo(line, nl=False)
                else:
                    sleep(0.5)
    except KeyboardInterrupt:
        typer.echo("Stopping log streaming. Services still running, use `stack down` to stop them.")
        pass


@app.command()
def up(
    foreground: bool = typer.Option(
        False,
        "--foreground",
        "-fg",
        help="Run the stack in the foreground (logs will be printed to console).",
    ),
    config: str = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to a YAML config file specifying generators and strategies to start.",
    ),
) -> None:
    """Start the trading stack."""
    typer.echo("Starting stack...")

    stack_config = {}
    if config:
        with open(config, "r") as f:
            stack_config = yaml.safe_load(f) or {}

    log = open("stack.log", "w")
    pids = {}

    # start the DataBus
    p = starter(
        "edts.pubsub:app", 8000, log, health_check=True, env={**os.environ, "NAME": "databus"}
    )
    pids["databus"] = {"pid": p.pid, "url": "http://localhost:8000"}
    typer.echo(f"DataBus started with PID {p.pid}")

    # start the DecisionBus
    p = starter(
        "edts.pubsub:app", 8001, log, health_check=True, env={**os.environ, "NAME": "decisionbus"}
    )
    pids["decisionbus"] = {"pid": p.pid, "url": "http://localhost:8001"}
    typer.echo(f"DecisionBus started with PID {p.pid}")

    # start the trader
    p = starter("edts.trader:app", 8002, log, health_check=True)
    pids["trader"] = {"pid": p.pid, "url": "http://localhost:8002"}
    typer.echo(f"Trader started with PID {p.pid}")

    # register trader as subscriber to the 'trader' topic on the DecisionBus
    r = requests.post("http://localhost:8001/topic/trader")
    r.raise_for_status()
    r = requests.post(
        "http://localhost:8001/subscribe/trader",
        params={"subscriber": "http://localhost:8002/message"},
    )
    r.raise_for_status()
    typer.echo("Trader subscribed to 'trader' topic on DecisionBus.")

    # start generators from config
    for gen_cfg in stack_config.get("generators", []):
        gen_name = gen_cfg["name"]
        gen_type = gen_cfg["type"]
        gen_port = gen_cfg.get("port", 8100)
        gen_kwargs = gen_cfg.get("kwargs", {})

        if gen_type not in GENERATOR_MODULES:
            typer.echo(
                f"""
                Unknown generator type '{gen_type}' for '{gen_name}'.
                Available: {list(GENERATOR_MODULES)}"""
            )
            raise typer.Exit(1)

        env = {**os.environ, **{k: str(v) for k, v in gen_kwargs.items()}}
        p = starter(GENERATOR_MODULES[gen_type], gen_port, log, health_check=True, env=env)
        pids.setdefault("generators", {})[gen_name] = {
            "pid": p.pid,
            "url": f"http://localhost:{gen_port}",
        }
        typer.echo(
            f"Generator '{gen_name}' ({gen_type}) started on port {gen_port} with PID {p.pid}."
        )

    # start strategies from config
    for strat_cfg in stack_config.get("strategies", []):
        strat_name = strat_cfg["name"]
        strat_type = strat_cfg["type"]
        strat_port = strat_cfg.get("port", 8200)
        strat_kwargs = strat_cfg.get("kwargs", {})
        subscribe_bus_url = strat_cfg["subscribe_bus_url"].rstrip("/")
        subscribe_topic = strat_cfg["subscribe_topic"]

        if strat_type not in STRATEGY_MODULES:
            typer.echo(
                f"Unknown strategy type '{strat_type}' for '{strat_name}'. "
                f"Available: {list(STRATEGY_MODULES)}"
            )
            raise typer.Exit(1)

        env = {**os.environ, **{k: str(v) for k, v in strat_kwargs.items()}}
        p = starter(STRATEGY_MODULES[strat_type], strat_port, log, health_check=True, env=env)
        pids.setdefault("strategies", {})[strat_name] = {
            "pid": p.pid,
            "url": f"http://localhost:{strat_port}",
        }
        typer.echo(
            f"Strategy '{strat_name}' ({strat_type}) started on port {strat_port} with PID {p.pid}."
        )

        # register publish topic on the DecisionBus
        publish_bus_url = strat_kwargs["PUBSUB_URL"].rstrip("/")
        publish_topic = strat_kwargs["PUBLISH_TOPIC"]
        r = requests.post(f"{publish_bus_url}/topic/{publish_topic}")
        r.raise_for_status()
        typer.echo(f"Registered topic '{publish_topic}' on {publish_bus_url}.")

        # subscribe strategy to the input topic on the DataBus
        receive_url = f"http://localhost:{strat_port}/message"
        r = requests.post(
            f"{subscribe_bus_url}/subscribe/{subscribe_topic}",
            params={"subscriber": receive_url},
        )
        r.raise_for_status()
        typer.echo(f"Subscribed '{receive_url}' to '{subscribe_topic}' on {subscribe_bus_url}.")

    with open(".stack.pids.yaml", "w") as f:
        yaml.dump(pids, f)
    typer.echo("Stack started successfully.")
    log.close()

    if foreground:
        stream_logs()


@app.command()
def down(
    force: bool = typer.Option(
        False, "--force", "-f", help="Force stop the stack without sending shutdown requests."
    ),
) -> None:
    """Stop the trading stack."""
    typer.echo("Stopping stack...")
    process_data = yaml.load(open(".stack.pids.yaml", "r"), Loader=yaml.SafeLoader)

    def _active(services: dict) -> dict:
        return {
            name: info
            for name, info in services.items()
            if subprocess.run(["kill", "-0", str(info["pid"])], capture_output=True).returncode == 0
        }

    active_generators = _active(process_data.get("generators", {}))
    active_strategies = _active(process_data.get("strategies", {}))

    if (active_generators or active_strategies) and not force:
        names = list(active_generators) + list(active_strategies)
        typer.echo(
            f"Active generators/strategies detected: {names}. "
            "Stop them first, or use `stack down --force`."
        )
        raise typer.Exit(1)

    for strat_name, pid_info in active_strategies.items():
        pid = pid_info["pid"]
        url = pid_info["url"]
        existed = stopper(url, pid, force)
        if existed:
            typer.echo(f"Strategy '{strat_name}' with PID {pid} stopped.")
        else:
            typer.echo(f"Strategy '{strat_name}' with PID {pid} already stopped.")

    for gen_name, pid_info in active_generators.items():
        pid = pid_info["pid"]
        url = pid_info["url"]
        existed = stopper(url, pid, force)
        if existed:
            typer.echo(f"Generator '{gen_name}' with PID {pid} stopped.")
        else:
            typer.echo(f"Generator '{gen_name}' with PID {pid} already stopped.")

    for process_name in ["trader", "decisionbus", "databus"]:
        pid_info = process_data.get(process_name)
        pid = pid_info["pid"]
        url = pid_info["url"]
        existed = stopper(url, pid, force)
        if existed:
            typer.echo(f"{process_name.capitalize()} with PID {pid} stopped.")
        else:
            typer.echo(f"{process_name.capitalize()} with PID {pid} already stopped.")
    typer.echo("Stack stopped successfully.")


def _load_pids() -> dict:
    if os.path.exists(".stack.pids.yaml"):
        return yaml.load(open(".stack.pids.yaml", "r"), Loader=yaml.SafeLoader) or {}
    return {}


def _save_pids(pids: dict) -> None:
    with open(".stack.pids.yaml", "w") as f:
        yaml.dump(pids, f)


def _parse_kwargs(kwargs: str) -> dict:
    env = {**os.environ}
    if kwargs:
        for kv in kwargs.split(","):
            k, v = kv.split("=", 1)
            env[k.strip()] = v.strip()
    return env


@generator_app.command("add")
def generator_add(
    name: str = typer.Argument(..., help="Generator name (e.g. random_walk)"),
    port: int = typer.Option(8100, "--port", "-p", help="Port to run the generator on."),
    kwargs: str = typer.Option(
        "", "--kwargs", "-k", help="Environment variable overrides as KEY=VAL,KEY2=VAL2."
    ),
) -> None:
    """Start a generator and register it with the stack."""
    if name not in GENERATOR_MODULES:
        typer.echo(f"Unknown generator '{name}'. Available: {list(GENERATOR_MODULES)}")
        raise typer.Exit(1)

    log = open("stack.log", "a")
    p = starter(GENERATOR_MODULES[name], port, log, health_check=True, env=_parse_kwargs(kwargs))
    log.close()

    pids = _load_pids()
    pids.setdefault("generators", {})[name] = {"pid": p.pid, "url": f"http://localhost:{port}"}
    _save_pids(pids)

    typer.echo(f"Generator '{name}' started on port {port} with PID {p.pid}.")


@generator_app.command("remove")
def generator_remove(
    name: str = typer.Argument(..., help="Generator name to stop."),
    force: bool = typer.Option(False, "--force", "-f", help="Force kill if graceful stop fails."),
) -> None:
    """Stop a running generator and remove it from the stack."""
    pids = _load_pids()
    pid_info = pids.get("generators", {}).get(name)
    if pid_info is None:
        typer.echo(f"Generator '{name}' not found in stack.")
        raise typer.Exit(1)

    existed = stopper(pid_info["url"], pid_info["pid"], force)
    if existed:
        typer.echo(f"Generator '{name}' with PID {pid_info['pid']} stopped.")
    else:
        typer.echo(f"Generator '{name}' with PID {pid_info['pid']} was already stopped.")

    del pids["generators"][name]
    _save_pids(pids)


@strategy_app.command("add")
def strategy_add(
    name: str = typer.Argument(..., help="Strategy name (e.g. mean_reversion)"),
    port: int = typer.Option(8200, "--port", "-p", help="Port to run the strategy on."),
    subscribe_bus_url: str = typer.Option(
        ..., "--subscribe-bus-url", help="PubSub URL to subscribe to for incoming messages."
    ),
    subscribe_topic: str = typer.Option(
        ..., "--subscribe-topic", help="Topic on the subscribe bus to listen to."
    ),
    kwargs: str = typer.Option(
        "", "--kwargs", "-k", help="Environment variable overrides as KEY=VAL,KEY2=VAL2."
    ),
) -> None:
    """Start a strategy, register its publish topic, and subscribe it to the input topic."""
    if name not in STRATEGY_MODULES:
        typer.echo(f"Unknown strategy '{name}'. Available: {list(STRATEGY_MODULES)}")
        raise typer.Exit(1)

    env = _parse_kwargs(kwargs)
    log = open("stack.log", "a")
    p = starter(STRATEGY_MODULES[name], port, log, health_check=True, env=env)
    log.close()

    pids = _load_pids()
    pids.setdefault("strategies", {})[name] = {"pid": p.pid, "url": f"http://localhost:{port}"}
    _save_pids(pids)
    typer.echo(f"Strategy '{name}' started on port {port} with PID {p.pid}.")

    # register publish topic on the DecisionBus
    publish_bus_url = env["PUBSUB_URL"].rstrip("/")
    publish_topic = env["PUBLISH_TOPIC"]
    r = requests.post(f"{publish_bus_url}/topic/{publish_topic}")
    r.raise_for_status()
    typer.echo(f"Registered topic '{publish_topic}' on {publish_bus_url}.")

    # subscribe strategy to the input topic
    receive_url = f"http://localhost:{port}/message"
    r = requests.post(
        f"{subscribe_bus_url.rstrip('/')}/subscribe/{subscribe_topic}",
        params={"subscriber": receive_url},
    )
    r.raise_for_status()
    typer.echo(f"Subscribed '{receive_url}' to '{subscribe_topic}' on {subscribe_bus_url}.")


@strategy_app.command("remove")
def strategy_remove(
    name: str = typer.Argument(..., help="Strategy name to stop."),
    force: bool = typer.Option(False, "--force", "-f", help="Force kill if graceful stop fails."),
) -> None:
    """Stop a running strategy and remove it from the stack."""
    pids = _load_pids()
    pid_info = pids.get("strategies", {}).get(name)
    if pid_info is None:
        typer.echo(f"Strategy '{name}' not found in stack.")
        raise typer.Exit(1)

    existed = stopper(pid_info["url"], pid_info["pid"], force)
    if existed:
        typer.echo(f"Strategy '{name}' with PID {pid_info['pid']} stopped.")
    else:
        typer.echo(f"Strategy '{name}' with PID {pid_info['pid']} was already stopped.")

    del pids["strategies"][name]
    _save_pids(pids)


@app.command()
def logs() -> None:
    stream_logs()


def main() -> None:
    app()


if __name__ == "__main__":
    main()
