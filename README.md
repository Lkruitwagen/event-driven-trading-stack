# event-driven-trading-stack
A lightweight demo of an event-driven trading stack.

## Concept

Let's keep things **super simple** - no docker, no docker-compose, no redis, no message queues, no cloud.

Just pure python: cli, FastAPI, and async.

Let's spin up pub/sub services that keep topics/subscribers _in memory_ and handle everything between https services.

## Useage

### CLI

Start `DataBus` and `DecisionBus` pubsubs, and the `Trader` process.

    stack start

Start with a given config yaml:

    stack start --config myconfig.yaml

Choose -foreground (default) /-background:

    stack start --fg

Add a DataGenerator:

    stack generator start <id>

Drop a DataGenerator:

    stack generator stop <id>

Start a Strategy:

    stack strategy start <id>

Drop a Strategy:

    stack strategy stop <id>

Check the running stack status:

    stack status

Stop the running stack:

    stack stop

### Config Object

## Installation

Requires [uv](https://docs.astral.sh/uv/).

```bash
uv venv --python 3.13
source .venv/bin/activate
uv pip install -e .
```

### Development

```bash
uv pip install -e ".[dev]"
pre-commit install
```

### Testing

```bash
pytest
```
