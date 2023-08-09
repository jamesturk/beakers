from databeakers._models import EdgeType
from pydantic import BaseModel
from databeakers.pipeline import Pipeline
from databeakers.beakers import TempBeaker


class IdOnly(BaseModel):
    pass


class Word(BaseModel):
    word: str | int  # allow int for testing error_map


class Sentence(BaseModel):
    sentence: str


def is_fruit(word: Word) -> bool:
    if word.word == "error":
        raise ValueError("error")
    elif word.word == "/0":
        raise ZeroDivisionError("zero")
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


fruits = Pipeline("fruits", "fruits_test.db")
fruits.add_beaker("word", Word)
fruits.add_beaker("normalized", Word, beaker_type=TempBeaker)
fruits.add_beaker("fruit", IdOnly)
fruits.add_beaker("sentence", Sentence)
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
    edge_type=EdgeType.conditional,
    error_map={(ValueError,): "errors"},
)
fruits.add_transform(
    "fruit",
    "sentence",
    lambda r: Sentence(sentence=f"{r['normalized'].word} is a delicious fruit."),
    whole_record=True,
)
fruits.add_seed(
    "abc",
    "word",
    lambda: [Word(word="apple"), Word(word="BANANA"), Word(word="cat")],
)
fruits.add_seed(
    "errors", "word", lambda: [Word(word=100), Word(word="pear"), Word(word="ERROR")]
)
