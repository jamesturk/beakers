import pytest
from databeakers.pipeline import Pipeline
from databeakers.beakers import TempBeaker, SqliteBeaker
from databeakers.exceptions import ItemNotFound
from examples import Word


@pytest.mark.parametrize("beakerCls", [TempBeaker, SqliteBeaker])
def test_beaker_repr(beakerCls):
    pipeline = Pipeline("test")
    beaker = beakerCls("test", Word, pipeline)
    assert repr(beaker) == f"{beakerCls.__name__}(test, Word)"


@pytest.mark.parametrize("beakerCls", [TempBeaker, SqliteBeaker])
def test_length(beakerCls):
    pipeline = Pipeline("test", ":memory:")
    beaker = beakerCls("test", Word, pipeline)
    assert len(beaker) == 0
    beaker.add_item(Word(word="one"), parent=None)
    assert len(beaker) == 1
    beaker.add_item(Word(word="two"), parent=None)
    assert len(beaker) == 2


@pytest.mark.parametrize("beakerCls", [TempBeaker, SqliteBeaker])
def test_items(beakerCls):
    pipeline = Pipeline("test", ":memory:")
    beaker = beakerCls("test", Word, pipeline)
    beaker.add_item(Word(word="one"), parent=None)
    beaker.add_item(Word(word="two"), parent=None)
    items = list(beaker.items())
    assert len(items[0][0]) == 36  # uuid
    assert items[0][1] == Word(word="one")
    assert len(items[1][0]) == 36  # uuid
    assert items[1][1] == Word(word="two")


@pytest.mark.parametrize("beakerCls", [TempBeaker, SqliteBeaker])
def test_delete_all(beakerCls):
    pipeline = Pipeline("test", ":memory:")
    beaker = beakerCls("test", Word, pipeline)
    beaker.add_item(Word(word="one"), parent="sr:a", id_="one")
    beaker.add_item(Word(word="two"), parent="sr:a", id_="two")
    assert len(beaker) == 2
    ids = beaker.delete()
    assert len(beaker) == 0
    assert ids == ["one", "two"]


@pytest.mark.parametrize("beakerCls", [TempBeaker, SqliteBeaker])
def test_all_ids(beakerCls):
    pipeline = Pipeline("test", ":memory:")
    beaker = beakerCls("test", Word, pipeline)
    words = [Word(word="one"), Word(word="two")]
    for word in words:
        beaker.add_item(word, parent=None)
    assert set(beaker.all_ids()) == {id for id, _ in beaker.items()}


@pytest.mark.parametrize("beakerCls", [TempBeaker, SqliteBeaker])
def test_getitem_basic(beakerCls):
    pipeline = Pipeline("test", ":memory:")
    beaker = beakerCls("test", Word, pipeline)
    words = [Word(word="one"), Word(word="two")]
    for word in words:
        beaker.add_item(word, parent=None)

    for id in beaker.all_ids():
        assert beaker.get_item(id) in words


@pytest.mark.parametrize("beakerCls", [TempBeaker, SqliteBeaker])
def test_getitem_missing(beakerCls):
    pipeline = Pipeline("test", ":memory:")
    beaker = beakerCls("test", Word, pipeline)

    with pytest.raises(ItemNotFound):
        beaker.get_item("missing")
