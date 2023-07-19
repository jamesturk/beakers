import pytest
from beakers import Recipe
from beakers.beakers import TempBeaker, SqliteBeaker
from beakers.exceptions import ItemNotFound
from testdata import Word


@pytest.mark.parametrize("beakerCls", [TempBeaker, SqliteBeaker])
def test_beaker_repr(beakerCls):
    recipe = Recipe("test")
    beaker = beakerCls("test", Word, recipe)
    assert repr(beaker) == f"{beakerCls.__name__}(test, Word)"


@pytest.mark.parametrize("beakerCls", [TempBeaker, SqliteBeaker])
def test_length(beakerCls):
    recipe = Recipe("test", ":memory:")
    beaker = beakerCls("test", Word, recipe)
    assert len(beaker) == 0
    beaker.add_item(Word(word="one"))
    assert len(beaker) == 1
    beaker.add_item(Word(word="two"))
    assert len(beaker) == 2


@pytest.mark.parametrize("beakerCls", [TempBeaker, SqliteBeaker])
def test_items(beakerCls):
    recipe = Recipe("test", ":memory:")
    beaker = beakerCls("test", Word, recipe)
    beaker.add_item(Word(word="one"))
    beaker.add_item(Word(word="two"))
    items = list(beaker.items())
    assert len(items[0][0]) == 36  # uuid
    assert items[0][1] == Word(word="one")
    assert len(items[1][0]) == 36  # uuid
    assert items[1][1] == Word(word="two")


@pytest.mark.parametrize("beakerCls", [TempBeaker, SqliteBeaker])
def test_reset(beakerCls):
    recipe = Recipe("test", ":memory:")
    beaker = beakerCls("test", Word, recipe)
    beaker.add_item(Word(word="one"))
    beaker.add_item(Word(word="two"))
    assert len(beaker) == 2
    beaker.reset()
    assert len(beaker) == 0


@pytest.mark.parametrize("beakerCls", [TempBeaker, SqliteBeaker])
def test_add_items(beakerCls):
    recipe = Recipe("test", ":memory:")
    beaker = beakerCls("test", Word, recipe)
    beaker.add_items([Word(word="one"), Word(word="two")])
    assert len(beaker) == 2


@pytest.mark.parametrize("beakerCls", [TempBeaker, SqliteBeaker])
def test_id_set(beakerCls):
    recipe = Recipe("test", ":memory:")
    beaker = beakerCls("test", Word, recipe)
    beaker.add_items([Word(word="one"), Word(word="two")])
    assert beaker.id_set() == {id for id, _ in beaker.items()}


@pytest.mark.parametrize("beakerCls", [TempBeaker, SqliteBeaker])
def test_getitem_basic(beakerCls):
    recipe = Recipe("test", ":memory:")
    beaker = beakerCls("test", Word, recipe)
    words = [Word(word="one"), Word(word="two")]
    beaker.add_items(words)

    for id in beaker.id_set():
        assert beaker.get_item(id) in words


@pytest.mark.parametrize("beakerCls", [TempBeaker, SqliteBeaker])
def test_getitem_missing(beakerCls):
    recipe = Recipe("test", ":memory:")
    beaker = beakerCls("test", Word, recipe)

    with pytest.raises(ItemNotFound):
        beaker.get_item("missing")
