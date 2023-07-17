from pathlib import Path
from pydantic import BaseModel
from beakers.recipe import Recipe


class Word(BaseModel):
    word: str


def is_fruit(word: Word) -> bool:
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
fruits.add_beaker("fruit", Word)
fruits.add_transform("word", "normalized", lambda x: x.lower())
fruits.add_transform("normalized", "fruit", is_fruit)
fruits.add_seed(
    "abc",
    "word",
    lambda: [Word(word="apple"), Word(word="BANANA"), Word(word="cat")],
)
