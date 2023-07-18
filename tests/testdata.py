from pathlib import Path
from pydantic import BaseModel
from beakers.recipe import Recipe


class IdOnly(BaseModel):
    pass


class Word(BaseModel):
    word: str | int  # allow int for testing error_map


def is_fruit(word: Word) -> bool:
    if word.word == "error":
        1 / 0
    return word.word in {
        "apple",
        "banana",
        "cherry",
        "durian",
        "elderberry",
        "fig",
        "grape",
        "honeydew",
        "jackfruit",
        "kiwi",
        "lemon",
        "mango",
        "nectarine",
        "orange",
        "pear",
        "quince",
        "raspberry",
        "strawberry",
        "tangerine",
        "watermelon",
    }


fruits = Recipe("fruits", "fruits_test.db")
fruits.add_beaker("word", Word)
fruits.add_beaker("normalized", Word)
fruits.add_beaker("fruit", IdOnly)
fruits.add_beaker("nonword", IdOnly)
fruits.add_beaker("errors", IdOnly)
fruits.add_transform(
    "word",
    "normalized",
    lambda x: Word(word=x.word.lower()),
    error_map={(AttributeError,): "nonword"},
)
fruits.add_transform(
    "normalized",
    "fruit",
    is_fruit,
    edge_type="conditional",
    error_map={(ZeroDivisionError,): "errors"},
)
fruits.add_seed(
    "abc",
    "word",
    lambda: [Word(word="apple"), Word(word="BANANA"), Word(word="cat")],
)
fruits.add_seed(
    "errors", "word", lambda: [Word(word=100), Word(word="pear"), Word(word="ERROR")]
)
