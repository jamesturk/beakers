from beakers import Recipe
from testdata import Word


def test_add_beaker_simple() -> None:
    recipe = Recipe("test")
    recipe.add_beaker("word", Word)
    assert recipe.beakers["word"].name == "word"
    assert recipe.beakers["word"].model == Word
