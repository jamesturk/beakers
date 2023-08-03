import pytest
from databeakers import Pipeline
from databeakers.pipeline import Edge, Seed, RunMode
from databeakers.exceptions import SeedError
from examples import Word, fruits


def capitalized(word: Word) -> Word:
    return Word(word=word.word.capitalize())  # type: ignore


@pytest.fixture
def pipeline():
    r = Pipeline("seed_test", ":memory:")
    r.add_beaker("word", Word)
    r.add_seed("one", "word", lambda: [Word(word="apple")])
    r.add_seed(
        "many",
        "word",
        lambda: [Word(word="banana"), Word(word="orange"), Word(word="pear")],
    )
    return r


def test_list_seeds_no_runs(pipeline):
    assert pipeline.list_seeds() == {
        "word": [Seed(name="one"), Seed(name="many")],
    }


def test_list_seeds_runs(pipeline):
    pipeline.run_seed("many")
    one, many = pipeline.list_seeds()["word"]
    assert one == Seed(name="one")
    assert many.name == "many"
    assert many.num_items == 3
    # 202x
    assert many.imported_at.startswith("202")


def test_run_seed_basic(pipeline):
    assert pipeline.run_seed("one") == 1
    assert len(pipeline.beakers["word"]) == 1
    assert pipeline.run_seed("many") == 3
    assert len(pipeline.beakers["word"]) == 4


def test_run_seed_bad_name(pipeline):
    with pytest.raises(SeedError):
        pipeline.run_seed("bad")


def test_run_seed_already_run(pipeline):
    pipeline.run_seed("one")
    assert len(pipeline.beakers["word"]) == 1
    with pytest.raises(SeedError):
        pipeline.run_seed("one")
    assert len(pipeline.beakers["word"]) == 1


def test_reset_seeds(pipeline):
    pipeline.run_seed("one")
    pipeline.run_seed("many")
    assert len(pipeline.beakers["word"]) == 4
    reset_list = pipeline.reset()
    assert reset_list == ["2 seeds", "word (4)"]
    assert len(pipeline.beakers["word"]) == 0


def test_pipeline_repr() -> None:
    pipeline = Pipeline("test")
    assert repr(pipeline) == "Pipeline(test)"


def test_add_beaker_simple() -> None:
    pipeline = Pipeline("test")
    pipeline.add_beaker("word", Word)
    assert pipeline.beakers["word"].name == "word"
    assert pipeline.beakers["word"].model == Word
    assert pipeline.beakers["word"].pipeline == pipeline


def test_add_transform():
    pipeline = Pipeline("test")
    pipeline.add_beaker("word", Word)
    pipeline.add_transform("word", "capitalized", capitalized)
    assert pipeline.graph["word"]["capitalized"]["edge"] == Edge(
        name="capitalized",
        func=capitalized,
        error_map={},
        edge_type="transform",
        whole_record=False,
    )


def test_add_transform_lambda():
    pipeline = Pipeline("test")
    pipeline.add_beaker("word", Word)
    pipeline.add_transform("word", "capitalized", lambda x: x)
    assert pipeline.graph["word"]["capitalized"]["edge"].name == "λ"


def test_add_transform_error_map():
    pipeline = Pipeline("test")
    pipeline.add_beaker("word", Word)
    pipeline.add_transform(
        "word", "capitalized", capitalized, error_map={(ValueError,): "error"}
    )
    assert pipeline.graph["word"]["capitalized"]["edge"].error_map == {
        (ValueError,): "error"
    }


def test_graph_data_simple():
    r = Pipeline("test")
    r.add_beaker("word", Word)
    r.add_beaker("capitalized", Word)
    r.add_beaker("filtered", Word)
    r.add_transform("word", "capitalized", capitalized)
    r.add_transform(
        "capitalized", "filtered", lambda x: x if x.word.startswith("A") else None
    )
    gd = r.graph_data()
    assert len(gd) == 3
    assert gd[0] == {
        "len": 0,
        "name": "word",
        "rank": 1,
        "temp": False,
        "edges": [
            {
                "to_beaker": "capitalized",
                "edge": Edge(
                    name="capitalized",
                    func=capitalized,
                    error_map={},
                    edge_type="transform",
                    whole_record=False,
                ),
            }
        ],
    }
    assert gd[1]["len"] == 0
    assert gd[1]["name"] == "capitalized"
    assert gd[1]["rank"] == 2
    assert gd[1]["temp"] is False
    assert gd[1]["edges"][0]["to_beaker"] == "filtered"
    assert gd[1]["edges"][0]["edge"].name == "λ"
    assert gd[2] == {
        "len": 0,
        "name": "filtered",
        "rank": 3,
        "temp": False,
        "edges": [],
    }


def test_graph_data_multiple_rank():
    r = Pipeline("test")
    r.add_beaker("nouns", Word)
    r.add_beaker("verbs", Word)
    r.add_beaker("normalized", Word)
    r.add_beaker("english", Word)
    r.add_beaker("spanish", Word)
    r.add_transform("nouns", "normalized", lambda x: x)
    r.add_transform("verbs", "normalized", lambda x: x)
    r.add_transform("normalized", "english", lambda x: x)
    r.add_transform("normalized", "spanish", lambda x: x)
    gd = r.graph_data()
    assert len(gd) == 5
    assert gd[0]["name"] == "nouns"
    assert gd[0]["rank"] == 1
    assert gd[1]["name"] == "verbs"
    assert gd[1]["rank"] == 1
    assert gd[2]["name"] == "normalized"
    assert gd[2]["rank"] == 2
    assert gd[3]["name"] == "english"
    assert gd[3]["rank"] == 3
    assert gd[4]["name"] == "spanish"
    assert gd[4]["rank"] == 3


@pytest.mark.parametrize("mode", [RunMode.waterfall, RunMode.river])
def test_run_waterfall(mode):
    fruits.reset()
    fruits.run_seed("abc")
    assert len(fruits.beakers["word"]) == 3
    report = fruits.run(mode)

    assert report.start_beaker is None
    assert report.end_beaker is None
    assert report.start_time is not None
    assert report.end_time is not None

    assert report.nodes["word"]["_already_processed"] == 0
    assert report.nodes["word"]["normalized"] == 3
    assert report.nodes["normalized"]["fruit"] == 2
    assert report.nodes["fruit"]["sentence"] == 2

    assert len(fruits.beakers["normalized"]) == 3
    assert len(fruits.beakers["fruit"]) == 2
    assert len(fruits.beakers["sentence"]) == 2


def test_run_twice():
    fruits.reset()
    fruits.run_seed("abc")
    assert len(fruits.beakers["word"]) == 3
    fruits.run(RunMode.waterfall)
    assert len(fruits.beakers["normalized"]) == 3
    assert len(fruits.beakers["fruit"]) == 2
    second_report = fruits.run(RunMode.waterfall)

    assert second_report.nodes["word"]["_already_processed"] == 3
    # TODO: this should be three, since the first run should have
    #      processed all three items, but it's two because the second run
    #      doesn't know about the items rejected by the filter.
    assert second_report.nodes["normalized"]["_already_processed"] == 2

    assert len(fruits.beakers["normalized"]) == 3
    assert len(fruits.beakers["fruit"]) == 2


@pytest.mark.parametrize("mode", [RunMode.waterfall, RunMode.river])
def test_run_errormap(mode):
    fruits.reset()
    fruits.run_seed("errors")  # [100, "pear", "ERROR"]
    assert len(fruits.beakers["word"]) == 3
    report = fruits.run(mode)

    # 100 winds up in non-words, two go on
    assert report.nodes["word"]["_already_processed"] == 0
    assert report.nodes["word"]["normalized"] == 2
    assert report.nodes["word"]["nonword"] == 1
    assert len(fruits.beakers["nonword"]) == 1
    assert len(fruits.beakers["normalized"]) == 2

    # ERROR winds up in errors, one goes on
    assert report.nodes["normalized"]["errors"] == 1
    assert report.nodes["normalized"]["fruit"] == 1
    assert len(fruits.beakers["errors"]) == 1
    assert len(fruits.beakers["fruit"]) == 1


@pytest.mark.parametrize("mode", [RunMode.waterfall, RunMode.river])
def test_run_error_out(mode):
    fruits.reset()

    # raise a zero division error, unhandled
    fruits.beakers["word"].add_item(Word(word="/0"))

    # uncaught error from is_fruit, propagates
    with pytest.raises(ZeroDivisionError):
        fruits.run(mode)
