import pytest
from databeakers.pipeline import Pipeline
from databeakers.exceptions import SeedError
from databeakers._models import Seed
from examples import Word


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


def test_reset_all_resets_seeds(pipeline):
    pipeline.run_seed("one")
    pipeline.run_seed("many")
    assert len(pipeline.beakers["word"]) == 4
    reset_list = pipeline.reset()
    assert reset_list == ["2 seeds", "word (4)"]
    assert len(pipeline.beakers["word"]) == 0


def test_run_seed_limit(pipeline):
    pipeline.run_seed("many", max_items=2)
    assert len(pipeline.beakers["word"]) == 2


def test_run_seed_reset(pipeline):
    pipeline.run_seed("many")
    assert len(pipeline.beakers["word"]) == 3
    pipeline.run_seed("many", reset=True)
    assert len(pipeline.beakers["word"]) == 3
