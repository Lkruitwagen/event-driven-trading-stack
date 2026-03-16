import typer

app = typer.Typer(help="Event-driven trading stack CLI.")


@app.command()
def up() -> None:
    """Start the trading stack."""
    typer.echo("Starting stack...")


@app.command()
def down() -> None:
    """Stop the trading stack."""
    typer.echo("Stopping stack...")


@app.command()
def status() -> None:
    """Show the status of the trading stack."""
    typer.echo("Stack status: not implemented")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
