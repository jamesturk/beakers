import pytest
import json
from typer.testing import CliRunner
from databeakers.cli import app
from databeakers.pipeline import RunMode
from examples import fruits, Sentence
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
    assert result.exit_code == 0
    assert "abc" in result.output
    assert "errors" in result.output
    assert "(-> word)" in result.output


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


def test_show_hidden_empty():
    fruits.reset()
    result = runner.invoke(app, ["--pipeline", "tests.examples.fruits", "show"])
    expected = """
┏━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Node ┃ Items ┃ Edges                    ┃
┡━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│      │       │                          │
│      │       │ (6 empty beakers hidden) │
└──────┴───────┴──────────────────────────┘
""".strip()
    print(result.output)
    assert result.output.strip() == expected


def test_show_empty():
    fruits.reset()
    result = runner.invoke(
        app, ["--pipeline", "tests.examples.fruits", "show", "--empty"]
    )
    expected = """
┏━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Node       ┃ Items ┃ Edges                      ┃
┡━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ errors     │     0 │                            │
│ fruit      │     0 │ λ -> sentence              │
│ nonword    │     0 │                            │
│ normalized │     0 │ is_fruit -> fruit / errors │
│ sentence   │     0 │                            │
│ word       │     0 │ λ -> normalized / nonword  │
└────────────┴───────┴────────────────────────────┘""".strip()
    print(result.output)
    assert result.output.strip() == expected


def test_show_some_data():
    fruits.reset()
    fruits.run_seed("abc")
    fruits.run_seed("errors")
    fruits.run(run_mode=RunMode.river)
    result = runner.invoke(
        app, ["--pipeline", "tests.examples.fruits", "show", "--empty", "--processed"]
    )
    expected = """
┏━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Node       ┃ Items ┃ Processed ┃ Edges                      ┃
┡━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ errors     │     1 │         - │                            │
│ fruit      │     3 │ 3  (100%) │ λ -> sentence              │
│ nonword    │     1 │         - │                            │
│ normalized │     0 │         - │ is_fruit -> fruit / errors │
│ sentence   │     3 │         - │                            │
│ word       │     6 │ 1  ( 17%) │ λ -> normalized / nonword  │
└────────────┴───────┴───────────┴────────────────────────────┘""".strip()
    print(result.output)
    assert result.output.strip() == expected


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


def test_output_json():
    fruits.reset()

    # TODO: there is a testing bug with TempBeakers so for now
    # just inject data into sentences
    fruits.beakers["sentence"].add_item(
        Sentence(sentence="apple is a delicious fruit."),
        parent="p",
        id_=1,
    )
    fruits.beakers["sentence"].add_item(
        Sentence(sentence="pineapple is a delicious fruit."),
        parent="p",
        id_=2,
    )

    result = runner.invoke(
        app,
        [
            "--pipeline",
            "tests.examples.fruits",
            "export",
            "--format",
            "json",
            "sentence",
        ],
    )
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert len(data) == 2
    assert data[0]["sentence"] == "apple is a delicious fruit."
    assert data[1]["sentence"] == "pineapple is a delicious fruit."


def test_output_csv():
    fruits.reset()

    # TODO: there is a testing bug with TempBeakers so for now
    # just inject data into sentences
    fruits.beakers["sentence"].add_item(
        Sentence(sentence="apple is a delicious fruit."),
        parent="p",
        id_=1,
    )
    fruits.beakers["sentence"].add_item(
        Sentence(sentence="pineapple is a delicious fruit."),
        parent="p",
        id_=2,
    )

    result = runner.invoke(
        app,
        [
            "--pipeline",
            "tests.examples.fruits",
            "export",
            "--format",
            "csv",
            "sentence",
        ],
    )
    assert result.exit_code == 0
    assert (
        result.stdout
        == "id,sentence\n1,apple is a delicious fruit.\n2,pineapple is a delicious fruit.\n"
    )


def test_peek_beaker():
    fruits.reset()
    runner.invoke(app, ["--pipeline", "tests.examples.fruits", "seed", "abc"])
    assert len(fruits.beakers["word"]) == 3

    result = runner.invoke(app, ["--pipeline", "tests.examples.fruits", "peek", "word"])
    assert result.exit_code == 0
    print(result.output)
    assert "word (3) " in result.output
    assert "apple" in result.output
    assert "BANANA" in result.output
    assert "cat" in result.output


def test_peek_beaker_join_beakers():
    fruits.reset()
    runner.invoke(app, ["--pipeline", "tests.examples.fruits", "seed", "abc"])
    runner.invoke(app, ["--pipeline", "tests.examples.fruits", "run"])
    assert len(fruits.beakers["word"]) == 3

    result = runner.invoke(
        app, ["--pipeline", "tests.examples.fruits", "peek", "word", "-b", "sentence"]
    )
    print(result.output)
    assert result.exit_code == 0
    assert "word (3) " in result.output
    assert "apple" in result.output
    assert "BANANA" in result.output
    assert "cat" in result.output
    assert "sentence_sentence" in result.output


def test_peek_beaker_params():
    fruits.reset()
    runner.invoke(app, ["--pipeline", "tests.examples.fruits", "seed", "abc"])
    assert len(fruits.beakers["word"]) == 3

    result = runner.invoke(
        app, ["--pipeline", "tests.examples.fruits", "peek", "word", "-p", "word=apple"]
    )
    assert result.exit_code == 0
    print(result.output)
    # assert "word [word=apple] (1) " in result.output
    assert "word [word=apple] (1)" in result.output
    assert "apple" in result.output
    assert "BANANA" not in result.output


def test_peek_item():
    fruits.reset()
    # full run through abc
    runner.invoke(app, ["--pipeline", "tests.examples.fruits", "seed", "abc"])
    ids = fruits.beakers["word"].all_ids()
    result = runner.invoke(app, ["--pipeline", "tests.examples.fruits", "run"])

    result = runner.invoke(app, ["--pipeline", "tests.examples.fruits", "peek", ids[0]])
    print(result.output)
    assert result.exit_code == 0
    assert ids[0] in result.output
    assert "apple is a delicious fruit" in result.output


def test_peek_item_bad_uuid():
    fruits.reset()

    result = runner.invoke(app, ["--pipeline", "tests.examples.fruits", "peek", "abc"])
    print(result.output)
    assert result.exit_code == 1
    assert "Unknown entity: abc" in result.output


def test_peek_item_non_existent():
    fruits.reset()

    bad_id = "00000000-0000-0000-0000-000000000000"
    result = runner.invoke(app, ["--pipeline", "tests.examples.fruits", "peek", bad_id])
    print(result.output)
    assert result.exit_code == 1
    assert bad_id in result.output


def test_peek_item_beaker():
    fruits.reset()
    # full run through abc
    runner.invoke(app, ["--pipeline", "tests.examples.fruits", "seed", "abc"])
    ids = fruits.beakers["word"].all_ids()
    result = runner.invoke(app, ["--pipeline", "tests.examples.fruits", "run"])

    result = runner.invoke(
        app, ["--pipeline", "tests.examples.fruits", "peek", ids[0] + ".word"]
    )
    print(result.output)
    assert result.exit_code == 0
    assert "apple" in result.output


def test_peek_item_beaker_record():
    fruits.reset()
    # full run through abc
    runner.invoke(app, ["--pipeline", "tests.examples.fruits", "seed", "abc"])
    ids = fruits.beakers["word"].all_ids()
    result = runner.invoke(app, ["--pipeline", "tests.examples.fruits", "run"])

    result = runner.invoke(
        app, ["--pipeline", "tests.examples.fruits", "peek", ids[0] + ".word.word"]
    )
    assert result.exit_code == 0
    assert "apple\n" == result.output
