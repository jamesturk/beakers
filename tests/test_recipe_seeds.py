from beakers import Recipe
from beakers.recipe import Seed
from beakers.exceptions import SeedError
from testdata import Word
import pytest


@pytest.fixture
def recipe():
    r = Recipe("seed_test", ":memory:")
    r.add_beaker("word", Word)
    r.add_seed("one", "word", lambda: [Word(word="apple")])
    r.add_seed(
        "many",
        "word",
        lambda: [Word(word="banana"), Word(word="orange"), Word(word="pear")],
    )
    return r


def test_list_seeds_no_runs(recipe):
    assert recipe.list_seeds() == {
        "word": [Seed(name="one"), Seed(name="many")],
    }


def test_list_seeds_runs(recipe):
    recipe.run_seed("many")
    one, many = recipe.list_seeds()["word"]
    assert one == Seed(name="one")
    assert many.name == "many"
    assert many.num_items == 3
    # 202x
    assert many.imported_at.startswith("202")


def test_run_seed_basic(recipe):
    assert recipe.run_seed("one") == 1
    assert len(recipe.beakers["word"]) == 1
    assert recipe.run_seed("many") == 3
    assert len(recipe.beakers["word"]) == 4


def test_run_seed_bad_name(recipe):
    with pytest.raises(SeedError):
        recipe.run_seed("bad")


def test_run_seed_already_run(recipe):
    recipe.run_seed("one")
    assert len(recipe.beakers["word"]) == 1
    with pytest.raises(SeedError):
        recipe.run_seed("one")
    assert len(recipe.beakers["word"]) == 1


def test_reset_seeds(recipe):
    recipe.run_seed("one")
    recipe.run_seed("many")
    assert len(recipe.beakers["word"]) == 4
    reset_list = recipe.reset()
    assert reset_list == ["2 seeds", "word (4)"]
    assert len(recipe.beakers["word"]) == 0
