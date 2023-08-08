import importlib
import time
import datetime
import typer
import sys
from types import SimpleNamespace
from rich import print
from rich.table import Table
from rich.text import Text
from rich.live import Live
from pprint import pprint
from typing import List, Optional
from typing_extensions import Annotated

from .models import RunMode
from .exceptions import SeedError

app = typer.Typer()


def _load_pipeline(dotted_path: str) -> SimpleNamespace:
    sys.path.append(".")
    path, name = dotted_path.rsplit(".", 1)
    mod = importlib.import_module(path)
    return getattr(mod, name)


@app.callback()
def main(
    ctx: typer.Context,
    pipeline: str = typer.Option(None, envvar="BEAKER_PIPELINE"),
) -> None:
    if not pipeline:
        typer.secho(
            "Missing pipeline; pass --pipeline or set env[BEAKER_PIPELINE]",
            fg=typer.colors.RED,
        )
        raise typer.Exit(1)
    ctx.obj = _load_pipeline(pipeline)


@app.command()
def reset(ctx: typer.Context) -> None:
    reset_list = ctx.obj.reset()
    if not reset_list:
        typer.secho("Nothing to reset!", fg=typer.colors.YELLOW)
        raise typer.Exit(1)
    for item in reset_list:
        typer.secho(f"Reset {item}", fg=typer.colors.RED)


@app.command()
def show(
    ctx: typer.Context,
    watch: bool = typer.Option(False, "--watch", "-w"),
) -> None:
    pipeline = ctx.obj

    def _make_table() -> Table:
        graph_data = pipeline.graph_data()
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Node")
        table.add_column("Items", justify="right")
        table.add_column("Edges")
        for node in graph_data:
            node_style = "green italic"
            if not node["temp"]:
                node_style = "green" if node["len"] else "green dim"
            edge_string = Text()
            for edge in node["edges"]:
                edge_string.append(
                    f"{edge['edge'].name} -> ",
                    style="cyan",
                )
                edge_string.append(
                    f"{edge['to_beaker']}",
                    style="green",
                )
                if edge["edge"].error_map:
                    for exceptions, to_beaker in edge["edge"].error_map.items():
                        edge_string.append(
                            f"\n   {' '.join(e.__name__ for e in exceptions)} -> {to_beaker}",
                            style="yellow",
                        )
            table.add_row(
                Text(f"{node['name']}", style=node_style),
                "-" if node["temp"] else str(node["len"]),
                edge_string,
            )
        return table

    if watch:
        with Live(_make_table(), refresh_per_second=1) as live:
            while True:
                time.sleep(1)
                live.update(_make_table())
    else:
        print(_make_table())


@app.command()
def graph(ctx: typer.Context) -> None:
    pprint(ctx.obj.graph_data())


@app.command()
def seeds(ctx: typer.Context) -> None:
    for beaker, seeds in ctx.obj.list_seeds().items():
        typer.secho(beaker)
        for seed in seeds:
            typer.secho(
                f"  {seed}",
                fg=typer.colors.GREEN if seed.num_items else typer.colors.YELLOW,
            )


@app.command()
def seed(ctx: typer.Context, name: str) -> None:
    try:
        start_time = time.time()
        num_items = ctx.obj.run_seed(name)
        duration = time.time() - start_time
        duration_dt = datetime.timedelta(seconds=duration)
        typer.secho(
            f"Seeded with {num_items} items in {duration_dt}", fg=typer.colors.GREEN
        )
    except SeedError as e:
        typer.secho(f"{e}", fg=typer.colors.RED)
        raise typer.Exit(1)


@app.command()
def run(
    ctx: typer.Context,
    input: Annotated[Optional[List[str]], typer.Option(...)] = None,
    start: Optional[str] = typer.Option(None),
    end: Optional[str] = typer.Option(None),
    mode: RunMode = typer.Option("waterfall"),
) -> None:
    has_data = any(ctx.obj.beakers.values())
    if not input and not has_data:
        typer.secho("No data! Run seed(s) first.", fg=typer.colors.RED)
        raise typer.Exit(1)
    report = ctx.obj.run(mode, start, end)

    table = Table(title="Run Report", show_header=False, show_lines=False)

    table.add_column("", style="cyan")
    table.add_column("")

    table.add_row("Start Time", report.start_time.strftime("%H:%M:%S %b %d"))
    table.add_row("End Time", report.end_time.strftime("%H:%M:%S %b %d"))
    duration = report.end_time - report.start_time
    table.add_row("Duration", str(duration))
    table.add_row("Start Beaker", report.start_beaker or "-")
    table.add_row("End Beaker", report.end_beaker or "-")
    table.add_row("Run Mode", report.run_mode.value)

    from_to_table = Table()
    from_to_table.add_column("From Beaker", style="cyan")
    from_to_table.add_column("Destinations")
    for from_beaker, to_beakers in report.nodes.items():
        destinations = "\n".join(
            f"{to_beaker} ({num_items})" for to_beaker, num_items in to_beakers.items()
        )
        if destinations:
            from_to_table.add_row(from_beaker, destinations)

    print(table)
    print(from_to_table)


@app.command()
def clear(
    ctx: typer.Context,
    beaker_name: str,
) -> None:
    if beaker_name not in ctx.obj.beakers:
        typer.secho(f"Beaker {beaker_name} not found", fg=typer.colors.RED)
        raise typer.Exit(1)
    else:
        beaker = ctx.obj.beakers[beaker_name]
        if typer.prompt(f"Clear {beaker_name} ({len(beaker)})? [y/N]") == "y":
            beaker.reset()
            typer.secho(f"Cleared {beaker_name}", fg=typer.colors.GREEN)


if __name__ == "__main__":  # pragma: no cover
    app()
