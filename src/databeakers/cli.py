import importlib
import time
import json
import csv
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
from .exceptions import SeedError, InvalidGraph
from .config import load_config
from .pipeline import Pipeline
from .beakers import TempBeaker
from .edges import Transform

# TODO: allow re-enabling locals (but is very slow/noisy w/ big text)
app = typer.Typer(pretty_exceptions_show_locals=False)


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
    try:
        ctx.obj = _load_pipeline(config.pipeline_path)
    except InvalidGraph as e:
        typer.secho(f"Invalid graph: {e}", fg=typer.colors.RED)
        raise typer.Exit(1)
    if not isinstance(ctx.obj, Pipeline):
        typer.secho(f"Invalid pipeline: {config.pipeline_path}")
        raise typer.Exit(1)


@app.command()
def show(
    ctx: typer.Context,
    watch: bool = typer.Option(False, "--watch", "-w"),
    empty: bool = typer.Option(False, "--empty"),
) -> None:
    """
    Show the current state of the pipeline.
    """

    def _make_table() -> Table:
        empty_count = 0
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Node")
        table.add_column("Items", justify="right")
        table.add_column("Processed", justify="right")
        table.add_column("Edges")
        for node in sorted(ctx.obj._beakers_toposort(None)):
            beaker = ctx.obj.beakers[node]
            length = len(beaker)
            if not length and not empty:
                empty_count += 1
                continue
            node_style = "dim italic"
            temp = True
            if not isinstance(beaker, TempBeaker):
                node_style = "green" if length else "green dim"
                temp = False
            edge_string = Text()
            first = True
            processed = set()
            for _, _, e in ctx.obj.graph.out_edges(node, data=True):
                if not first:
                    edge_string.append("\n")
                first = False

                edge = e["edge"]
                processed |= ctx.obj._all_upstream_ids(edge)

                if isinstance(edge, Transform):
                    edge_string.append(f"{edge.name} -> {edge.to_beaker}", style="cyan")
                    for exceptions, to_beaker in edge.error_map.items():
                        edge_string.append(
                            f"\n   {' '.join(e.__name__ for e in exceptions)} -> {to_beaker}",
                            style="yellow",
                        )
                else:
                    edge_string.append(f"{edge.name}", style="cyan")
                    for edge in edge.splitter_map.values():
                        edge_string.append(f"\n   -> {edge.to_beaker}", style="green")

            # calculate display string for processed
            processed &= set(beaker.all_ids())
            if temp or first:  # temp beaker or no edges
                processed_str = Text("-", style="dim")
            elif len(processed):
                processed_str = Text(
                    f"{len(processed)}  ({len(processed) / length:>4.0%})",
                    style="green" if len(processed) == length else "yellow",
                )
            else:
                processed_str = Text("0   (  0%)", style="dim red")
            table.add_row(
                Text(f"{node}", style=node_style),
                str(length),
                processed_str,
                edge_string,
            )

        if empty_count:
            table.add_row("", "", "", f"\n({empty_count} empty beakers hidden)")

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
    ctx: typer.Context,
    filename: str = typer.Option("graph.svg", "--filename", "-f"),
    excludes: list[str] = typer.Option([], "--exclude", "-e"),
) -> None:
    """
    Write a graphviz graph of the pipeline to a file.
    """
    dotg = ctx.obj.to_pydot(excludes)
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
        for seed, runs in seeds.items():
            typer.secho(
                f"  {seed}",
                fg=typer.colors.GREEN if len(runs) else typer.colors.YELLOW,
            )
            for run in runs:
                typer.secho(f"    {run}", fg=typer.colors.GREEN)


@app.command()
def seed(
    ctx: typer.Context,
    name: str,
    num_items: int = typer.Option(0, "--num-items", "-n"),
    reset: bool = typer.Option(False, "--reset", "-r"),
) -> None:
    """
    Run a seed.
    """
    try:
        seed_run = ctx.obj.run_seed(name, max_items=num_items, reset=reset)
        typer.secho(f"Ran seed: {seed_run}", fg=typer.colors.GREEN)
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
    offset: int = typer.Option(0, "--offset", "-o"),
    max_items: int = typer.Option(10, "--max-items", "-n"),
    extra_beakers: list[str] = typer.Option([], "--beaker", "-b"),
):
    """
    Peek at a beaker or record.
    """
    if not thing:
        typer.secho("Must specify a beaker name or UUID", fg=typer.colors.RED)
        raise typer.Exit(1)
    elif thing in ctx.obj.beakers:
        beaker = ctx.obj.beakers[thing]
        beakers = [thing] + extra_beakers  # list of all names
        t = Table(title=f"{thing} ({len(beaker)})", show_header=True, show_lines=False)
        t.add_column("UUID", style="cyan")
        rows = list(ctx.obj._grab_rows(beakers, offset=offset, max_items=max_items))
        for field in rows[0].keys():
            t.add_column(field)
        for rec in rows:
            fields = [rec["id"]]
            for field, value in rec.items():
                if field == "id":
                    continue
                if isinstance(value, str):
                    value = (
                        value[:40] + f"... ({len(value)})" if len(value) > 40 else value
                    )
                fields.append(str(value))
            t.add_row(*fields)
        print(t)
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


@app.command()
def export(
    ctx: typer.Context,
    beakers: list[str],
    format: str = typer.Option("json", "--format", "-f"),
    max_items: int = typer.Option(0, "--max-items", "-n"),
    offset: int = typer.Option(0, "--offset", "-o"),
) -> None:
    """
    Export data from beakers.
    """
    output = list(ctx.obj._grab_rows(beakers, max_items=max_items, offset=offset))

    if format == "json":
        print(json.dumps(output, indent=1))
    elif format == "csv":
        writer = csv.DictWriter(
            sys.stdout,
            fieldnames=[k for k in output[0].keys()],
        )
        writer.writeheader()
        writer.writerows(output)


if __name__ == "__main__":  # pragma: no cover
    app()
