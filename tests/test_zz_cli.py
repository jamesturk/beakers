import pytest
from typer.testing import CliRunner
from databeakers.cli import app
from examples import fruits
import os

"""
This file is named test_zz_cli.py so that it runs last.

These are basically E2E tests & not as isolated as other unit tests.
If they fail check for failing unit tests first!

TODO: each fruits.reset() call could be replaced if there were a global CLI flag to
overwrite the database.
"""

runner = CliRunner()


@pytest.fixture
def no_color():
    os.environ["NO_COLOR"] = "1"


def test_no_pipeline():
    result = runner.invoke(app, ["seeds"])
    assert (
        result.output
        == "Missing pipeline; pass --pipeline or set env[databeakers_pipeline_path]\n"
    )
    assert result.exit_code == 1


def test_list_seeds_simple(no_color):
    fruits.reset()
    result = runner.invoke(app, ["--pipeline", "tests.examples.fruits", "seeds"])
    assert "word\n  abc\n  errors\n" in result.output
    assert result.exit_code == 0


def test_run_seed_simple():
    fruits.reset()
    result = runner.invoke(app, ["--pipeline", "tests.examples.fruits", "seed", "abc"])
    assert "num_items=3" in result.output
    assert "seed_name=abc" in result.output
    assert result.exit_code == 0
    assert len(fruits.beakers["word"]) == 3


def test_run_seed_twice():
    fruits.reset()
    runner.invoke(app, ["--pipeline", "tests.examples.fruits", "seed", "abc"])
    result = runner.invoke(app, ["--pipeline", "tests.examples.fruits", "seed", "abc"])
    assert "abc already run" in result.output
    assert result.exit_code == 1


def test_clear_all():
    fruits.reset()
    runner.invoke(app, ["--pipeline", "tests.examples.fruits", "seed", "abc"])
    result = runner.invoke(
        app, ["--pipeline", "tests.examples.fruits", "clear", "--all"]
    )
    assert result.output == "Reset word (3)\n"
    assert result.exit_code == 0


def test_clear_nothing():
    fruits.reset()
    result = runner.invoke(
        app, ["--pipeline", "tests.examples.fruits", "clear", "--all"]
    )
    assert result.output == "Nothing to reset!\n"
    assert result.exit_code == 1


def test_show():
    fruits.reset()
    result = runner.invoke(app, ["--pipeline", "tests.examples.fruits", "show"])
    assert (
        result.output
        == """┏━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Node ┃ Items ┃ Edges                    ┃
┡━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│      │       │ (6 empty beakers hidden) │
└──────┴───────┴──────────────────────────┘
"""
    )


def test_show_empty():
    fruits.reset()
    result = runner.invoke(
        app, ["--pipeline", "tests.examples.fruits", "show", "--empty"]
    )
    assert (
        result.output
        == """┏━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Node       ┃ Items ┃ Edges                        ┃
┡━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ errors     │     0 │                              │
│ fruit      │     0 │ λ -> sentence                │
│ nonword    │     0 │                              │
│ normalized │     - │ is_fruit -> fruit            │
│            │       │    ValueError -> errors      │
│ sentence   │     0 │                              │
│ word       │     0 │ λ -> normalized              │
│            │       │    AttributeError -> nonword │
└────────────┴───────┴──────────────────────────────┘
"""
    )


def test_run_no_data():
    fruits.reset()
    result = runner.invoke(app, ["--pipeline", "tests.examples.fruits", "run"])
    assert result.output == "No data! Run seed(s) first.\n"
    assert result.exit_code == 1


def test_run_simple():
    fruits.reset()
    runner.invoke(app, ["--pipeline", "tests.examples.fruits", "seed", "abc"])
    result = runner.invoke(
        app, ["--pipeline", "tests.examples.fruits", "--log-level", "info", "run"]
    )
    # logs
    assert "edge" in result.output
    assert "is_fruit" in result.output
    assert result.exit_code == 0
    assert len(fruits.beakers["word"]) == 3
    assert len(fruits.beakers["fruit"]) == 2
    # can't see normalized because it's a TempBeaker & will be empty
    assert len(fruits.beakers["normalized"]) == 0

    assert "Run Report" in result.output
    assert "word" in result.output
    assert "fruit (2)" in result.output
    assert "sentence (2)" in result.output
