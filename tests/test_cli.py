from typer.testing import CliRunner
from databeakers.cli import app
from examples import fruits

"""
These are basically E2E tests & not as isolated as other unit tests.
If they fail check for failing unit tests first!

TODO: each fruits.reset() call could be replaced if there were a global CLI flag to
overwrite the database.
"""

runner = CliRunner()


def test_no_pipeline():
    result = runner.invoke(app, ["seeds"])
    assert (
        result.output
        == "Missing pipeline; pass --pipeline or set env[databeakers_pipeline_path]\n"
    )
    assert result.exit_code == 1


def test_list_seeds_simple():
    fruits.reset()
    result = runner.invoke(app, ["--pipeline", "tests.examples.fruits", "seeds"])
    assert "word\n  abc\n  errors\n" in result.output
    assert result.exit_code == 0


def test_run_seed_simple():
    fruits.reset()
    result = runner.invoke(app, ["--pipeline", "tests.examples.fruits", "seed", "abc"])
    assert "3 items" in result.output
    assert result.exit_code == 0
    assert len(fruits.beakers["word"]) == 3


def test_run_seed_twice():
    fruits.reset()
    runner.invoke(app, ["--pipeline", "tests.examples.fruits", "seed", "abc"])
    result = runner.invoke(app, ["--pipeline", "tests.examples.fruits", "seed", "abc"])
    assert "abc already run at" in result.output
    assert result.exit_code == 1


def test_reset_seeds():
    fruits.reset()
    runner.invoke(app, ["--pipeline", "tests.examples.fruits", "seed", "abc"])
    result = runner.invoke(app, ["--pipeline", "tests.examples.fruits", "reset"])
    assert result.output == "Reset 1 seeds\nReset word (3)\n"
    assert result.exit_code == 0


def test_reset_nothing():
    fruits.reset()
    result = runner.invoke(app, ["--pipeline", "tests.examples.fruits", "reset"])
    assert result.output == "Nothing to reset!\n"
    assert result.exit_code == 1


def test_show():
    fruits.reset()
    result = runner.invoke(app, ["--pipeline", "tests.examples.fruits", "show"])
    assert (
        result.output
        == """┏━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Node       ┃ Items ┃ Edges                        ┃
┡━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ errors     │     0 │                              │
│ nonword    │     0 │                              │
│ word       │     0 │ λ -> normalized              │
│            │       │    AttributeError -> nonword │
│ normalized │     - │ is_fruit -> fruit            │
│            │       │    ValueError -> errors      │
│ fruit      │     0 │ λ -> sentence                │
│ sentence   │     0 │                              │
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
    result = runner.invoke(app, ["--pipeline", "tests.examples.fruits", "run"])
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
