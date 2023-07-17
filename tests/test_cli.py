from typer.testing import CliRunner
from beakers.cli import app
from testdata import fruits

"""
These are basically E2E tests & not as isolated as other unit tests.
If they fail check for failing unit tests first!

TODO: each fruits.reset() call could be replaced if there were a global CLI flag to
overwrite the database.
"""

runner = CliRunner()


def test_no_recipe():
    result = runner.invoke(app, ["seeds"])
    assert result.output == "Missing recipe; pass --recipe or set env[BEAKER_RECIPE]\n"
    assert result.exit_code == 1


def test_list_seeds_simple():
    fruits.reset()
    result = runner.invoke(app, ["--recipe", "tests.testdata.fruits", "seeds"])
    assert result.output == "word\n  abc\n"
    assert result.exit_code == 0


def test_run_seed_simple():
    fruits.reset()
    result = runner.invoke(app, ["--recipe", "tests.testdata.fruits", "seed", "abc"])
    assert "3 items" in result.output
    assert result.exit_code == 0
    assert len(fruits.beakers["word"]) == 3


def test_run_seed_twice():
    fruits.reset()
    runner.invoke(app, ["--recipe", "tests.testdata.fruits", "seed", "abc"])
    result = runner.invoke(app, ["--recipe", "tests.testdata.fruits", "seed", "abc"])
    assert "abc already run at" in result.output
    assert result.exit_code == 1


def test_reset_seeds():
    fruits.reset()
    runner.invoke(app, ["--recipe", "tests.testdata.fruits", "seed", "abc"])
    result = runner.invoke(app, ["--recipe", "tests.testdata.fruits", "reset"])
    assert result.output == "Reset 1 seeds\nReset word (3)\n"
    assert result.exit_code == 0


def test_reset_nothing():
    fruits.reset()
    result = runner.invoke(app, ["--recipe", "tests.testdata.fruits", "reset"])
    assert result.output == "Nothing to reset!\n"
    assert result.exit_code == 1


def test_show():
    fruits.reset()
    result = runner.invoke(app, ["--recipe", "tests.testdata.fruits", "show"])
    assert (
        result.output
        == """word (0)
  -(λ)-> normalized
normalized (0)
  -(is_fruit)-> fruit
fruit (0)
"""
    )
