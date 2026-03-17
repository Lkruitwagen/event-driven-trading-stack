import subprocess
from io import TextIOWrapper
from time import sleep

import requests
import typer
import yaml

STARTUP_TIMEOUT = 10  # seconds


app = typer.Typer(help="Event-driven trading stack CLI.")


def starter(module: str, port: int, output: int | TextIOWrapper, health_check: bool = True):
    p = subprocess.Popen(
        ["uvicorn", f"{module}", "--port", str(port)],
        stdout=output,
        stderr=output,
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
    # gracefully stop the service
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
        "-f",
        help="Run the stack in the foreground (logs will be printed to console).",
    ),
) -> None:
    """Start the trading stack."""
    typer.echo("Starting stack...")

    log = open("stack.log", "w")
    pids = {}

    # start the DataBus
    p = starter("edts.pubsub:app", 8000, log, health_check=True)
    pids["databus"] = {"pid": p.pid, "url": "http://localhost:8000"}

    typer.echo(f"DataBus started with PID {p.pid}")

    # start the DecisionBus
    p = starter("edts.pubsub:app", 8001, log, health_check=True)
    pids["decisionbus"] = {"pid": p.pid, "url": "http://localhost:8001"}
    typer.echo(f"DecisionBus started with PID {p.pid}")

    # start the trader
    p = starter("edts.trader:app", 8002, log, health_check=True)
    pids["trader"] = {"pid": p.pid, "url": "http://localhost:8002"}
    typer.echo(f"Trader started with PID {p.pid}")

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


@app.command()
def status() -> None:
    """Show the status of the trading stack."""
    typer.echo("Stack status: not implemented")


@app.command()
def logs() -> None:
    stream_logs()


def main() -> None:
    app()


if __name__ == "__main__":
    main()
