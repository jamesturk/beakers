import importlib
import time
import datetime
import typer
import sys
import re
from types import SimpleNamespace
from rich import print
from rich.table import Table
from rich.text import Text
from rich.live import Live
from typing import List, Optional
from typing_extensions import Annotated

from ._models import RunMode
from .exceptions import SeedError
from .config import load_config
from .pipeline import Pipeline

app = typer.Typer()


def _load_pipeline(dotted_path: str) -> SimpleNamespace:
    sys.path.append(".")
    path, name = dotted_path.rsplit(".", 1)
    mod = importlib.import_module(path)
    return getattr(mod, name)


@app.callback()
def main(
    ctx: typer.Context,
    pipeline: str = typer.Option(""),
    log_level: str = typer.Option(""),
) -> None:
    overrides = {"pipeline_path": pipeline}
    if log_level:
        overrides["log_level"] = log_level
    config = load_config(**overrides)
    if not config.pipeline_path:
        typer.secho(
            "Missing pipeline; pass --pipeline or set env[databeakers_pipeline_path]",
            fg=typer.colors.RED,
        )
        raise typer.Exit(1)
    ctx.obj = _load_pipeline(config.pipeline_path)
    if not isinstance(ctx.obj, Pipeline):
        typer.secho(f"Invalid pipeline: {config.pipeline_path}")
        raise typer.Exit(1)


@app.command()
def show(
    ctx: typer.Context,
    watch: bool = typer.Option(False, "--watch", "-w"),
) -> None:
    """
    Show the current state of the pipeline.
    """
    pipeline = ctx.obj

    def _make_table() -> Table:
        graph_data = pipeline.graph_data()
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Node")
        table.add_column("Items", justify="right")
        table.add_column("Edges")
        for node in graph_data:
            node_style = "dim italic"
            if not node["temp"]:
                node_style = "green" if node["len"] else "green dim"
            edge_string = Text()
            first = True
            for edge in node["edges"]:
                if not first:
                    edge_string.append("\n")
                first = False
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
def graph(
    ctx: typer.Context, filename: str = typer.Option("graph.svg", "--filename", "-f")
) -> None:
    dotg = ctx.obj.to_pydot()
    if filename.endswith(".svg"):
        dotg.write_svg(filename, prog="dot")
    elif filename.endswith(".png"):
        dotg.write_png(filename, prog="dot")
    elif filename.endswith(".dot"):
        # maybe write_raw instead?
        dotg.write_dot(filename)
    else:
        typer.secho(f"Unknown file extension: {filename}", fg=typer.colors.RED)
        raise typer.Exit(1)
    typer.secho(f"Graph written to {filename}", fg=typer.colors.GREEN)


@app.command()
def seeds(ctx: typer.Context) -> None:
    """
    List the available seeds and their status.
    """
    for beaker, seeds in ctx.obj.list_seeds().items():
        typer.secho(beaker)
        for seed in seeds:
            typer.secho(
                f"  {seed}",
                fg=typer.colors.GREEN if seed.num_items else typer.colors.YELLOW,
            )


@app.command()
def seed(ctx: typer.Context, name: str) -> None:
    """
    Run a seed.
    """
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
    only: Annotated[Optional[List[str]], typer.Option(...)] = None,
    mode: RunMode = typer.Option("waterfall"),
) -> None:
    """
    Execute the pipeline, or a part of it.
    """
    has_data = any(ctx.obj.beakers.values())
    if not has_data:
        typer.secho("No data! Run seed(s) first.", fg=typer.colors.RED)
        raise typer.Exit(1)
    report = ctx.obj.run(mode, only)

    table = Table(title="Run Report", show_header=False, show_lines=False)

    table.add_column("", style="cyan")
    table.add_column("")

    table.add_row("Start Time", report.start_time.strftime("%H:%M:%S %b %d"))
    table.add_row("End Time", report.end_time.strftime("%H:%M:%S %b %d"))
    duration = report.end_time - report.start_time
    table.add_row("Duration", str(duration))
    table.add_row("Beakers", ", ".join(report.only_beakers) or "(all)")
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
    beaker_name: Optional[str] = typer.Argument(None),
    all: bool = typer.Option(False, "--all", "-a"),
) -> None:
    """
    Clear a beaker's data.
    """
    if all:
        reset_list = ctx.obj.reset()
        if not reset_list:
            typer.secho("Nothing to reset!", fg=typer.colors.YELLOW)
            raise typer.Exit(1)
        for item in reset_list:
            typer.secho(f"Reset {item}", fg=typer.colors.RED)
        return

    if not beaker_name:
        typer.secho("Must specify a beaker name", fg=typer.colors.RED)

    if beaker_name not in ctx.obj.beakers:
        typer.secho(f"Beaker {beaker_name} not found", fg=typer.colors.RED)
        raise typer.Exit(1)
    else:
        beaker = ctx.obj.beakers[beaker_name]
        if typer.prompt(f"Clear {beaker_name} ({len(beaker)})? [y/N]") == "y":
            beaker.reset()
            typer.secho(f"Cleared {beaker_name}", fg=typer.colors.GREEN)


uuid_re = re.compile(r"^[0-9a-f]{8}-([0-9a-f]{4}-){3}[0-9a-f]{12}$")


@app.command()
def peek(
    ctx: typer.Context,
    thing: Optional[str] = typer.Argument(None),
):
    if not thing:
        typer.secho("Must specify a beaker name or UUID", fg=typer.colors.RED)
        raise typer.Exit(1)
    elif uuid_re.match(thing):
        record = ctx.obj._get_full_record(thing)
        t = Table(title=thing, show_header=False, show_lines=False)
        t.add_column("Beaker", style="cyan")
        t.add_column("Field")
        t.add_column("Value")
        for beaker_name in ctx.obj.beakers:
            try:
                record[beaker_name]
                t.add_row(beaker_name, "", "")
                for field in record[beaker_name].model_fields:
                    value = getattr(record[beaker_name], field)
                    if isinstance(value, str):
                        value = (
                            value[:20] + f"... ({len(value)})"
                            if len(value) > 20
                            else value
                        )
                    t.add_row("", field, str(value))
            except KeyError:
                pass
        print(t)
    else:
        typer.secho(f"Unknown entity: {thing}", fg=typer.colors.RED)
        raise typer.Exit(1)


if __name__ == "__main__":  # pragma: no cover
    app()
