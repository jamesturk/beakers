import pytest
from databeakers.pipeline import Pipeline
from databeakers.exceptions import SeedError
from examples import Word


def places():
    yield Word(word="north carolina")
    yield Word(word="new york")
    yield Word(word="montana")
    yield Word(word="washington dc")
    yield Word(word="illinois")


def farm():
    yield Word(word="cow")
    yield Word(word="pig")
    yield Word(word="chicken")
    yield Word(word="horse")
    yield Word(word="goat")


def zoo():
    yield Word(word="lion")
    yield Word(word="tiger")
    yield Word(word="bear")
    yield Word(word="elephant")
    yield Word(word="giraffe")
    yield Word(word="zebra")
    yield Word(word="monkey")
    yield Word(word="gorilla")
    yield Word(word="penguin")


@pytest.fixture
def pipeline():
    p = Pipeline("seeds", ":memory:")
    p.add_beaker("animal", Word)
    p.add_beaker("place", Word)
    p.register_seed(places, "place")
    p.register_seed(farm, "animal")
    p.register_seed(zoo, "animal")
    return p


def test_list_seeds(pipeline):
    res = pipeline.list_seeds()
    assert res == {"animal": {"farm": [], "zoo": []}, "place": {"places": []}}


def test_run_seed(pipeline):
    pipeline.run_seed("places")

    assert len(pipeline.beakers["place"]) == 5


def test_run_two_seeds(pipeline):
    pipeline.run_seed("farm")
    pipeline.run_seed("zoo")

    assert len(pipeline.beakers["animal"]) == 14


def test_run_seed_bad_name(pipeline):
    with pytest.raises(SeedError):
        pipeline.run_seed("bad")


def test_run_seed_already_run(pipeline):
    pipeline.run_seed("farm")
    with pytest.raises(SeedError):
        pipeline.run_seed("farm")
    assert len(pipeline.beakers["animal"]) == 5


def test_reset_all_resets_seeds(pipeline):
    pipeline.run_seed("farm")
    pipeline.run_seed("zoo")
    assert len(pipeline.beakers["animal"]) == 14
    pipeline.reset()
    assert len(pipeline.beakers["animal"]) == 0


def test_run_seed_limit(pipeline):
    pipeline.run_seed("zoo", max_items=2)
    assert len(pipeline.beakers["animal"]) == 2


def test_run_seed_reset(pipeline):
    pipeline.run_seed("farm")
    assert len(pipeline.beakers["animal"]) == 5
    pipeline.run_seed("farm", reset=True)
    assert len(pipeline.beakers["animal"]) == 5


def test_get_run(pipeline):
    pipeline.run_seed("places")

    run = pipeline.get_seed_run("sr:places")
    assert run.beaker_name == "place"
    assert run.seed_name == "places"
    assert run.run_repr == "sr:places"
    assert run.num_items == 5
    assert run.start_time is not None
    assert run.end_time is not None
