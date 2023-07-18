from collections import Counter
import importlib
import typer
import sys
from types import SimpleNamespace
from pprint import pprint
from typing import List, Optional
from typing_extensions import Annotated

from beakers.exceptions import SeedError

app = typer.Typer()


def _load_recipe(dotted_path: str) -> SimpleNamespace:
    sys.path.append(".")
    path, name = dotted_path.rsplit(".", 1)
    mod = importlib.import_module(path)
    return getattr(mod, name)


@app.callback()
def main(
    ctx: typer.Context,
    recipe: str = typer.Option(None, envvar="BEAKER_RECIPE"),
) -> None:
    if not recipe:
        typer.secho(
            "Missing recipe; pass --recipe or set env[BEAKER_RECIPE]",
            fg=typer.colors.RED,
        )
        raise typer.Exit(1)
    ctx.obj = _load_recipe(recipe)


@app.command()
def reset(ctx: typer.Context) -> None:
    reset_list = ctx.obj.reset()
    if not reset_list:
        typer.secho("Nothing to reset!", fg=typer.colors.YELLOW)
        raise typer.Exit(1)
    for item in reset_list:
        typer.secho(f"Reset {item}", fg=typer.colors.RED)


@app.command()
def show(ctx: typer.Context) -> None:
    recipe = ctx.obj
    seed_count = Counter(recipe.seeds.keys())
    graph_data = recipe.graph_data()
    for node in graph_data:
        if node["temp"]:
            typer.secho(node["name"], fg=typer.colors.CYAN)
        else:
            typer.secho(
                f"{node['name']} ({node['len']})",
                fg=typer.colors.GREEN if node["len"] else typer.colors.YELLOW,
            )
        for edge in node["edges"]:
            typer.secho(f"  -({edge['edge'].name})-> {edge['to_beaker']}")
            for k, v in edge["edge"].error_map.items():
                if isinstance(k, tuple):
                    typer.secho(
                        f"    {' '.join(c.__name__ for c in k)} -> {v}",
                        fg=typer.colors.RED,
                    )
                else:
                    typer.secho(f"    {k.__name__} -> {v}", fg=typer.colors.RED)


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
        num_items = ctx.obj.run_seed(name)
        typer.secho(f"Seeded with {num_items} items", fg=typer.colors.GREEN)
    except SeedError as e:
        typer.secho(f"{e}", fg=typer.colors.RED)
        raise typer.Exit(1)


@app.command()
def run(
    ctx: typer.Context,
    input: Annotated[Optional[List[str]], typer.Option(...)] = None,
    start: Optional[str] = typer.Option(None),
    end: Optional[str] = typer.Option(None),
) -> None:
    has_data = any(ctx.obj.beakers.values())
    if not input and not has_data:
        typer.secho("No data! Run seed(s) first.", fg=typer.colors.RED)
        raise typer.Exit(1)
    ctx.obj.run_once(start, end)


if __name__ == "__main__":  # pragma: no cover
    app()
