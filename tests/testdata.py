from pathlib import Path
from pydantic import BaseModel
from beakers.recipe import Recipe


class Word(BaseModel):
    word: str


class Flag(BaseModel):
    flag: bool


def is_fruit(word: Word) -> Flag:
    return Flag(
        flag=word.word
        in {
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
    )


fruits = Recipe("fruits", "fruits_test.db")
fruits.add_beaker("word", Word)
fruits.add_beaker("normalized", Word)
fruits.add_beaker("fruit", Flag)
fruits.add_transform("word", "normalized", lambda x: Word(word=x.word.lower()))
fruits.add_transform("normalized", "fruit", is_fruit)
fruits.add_seed(
    "abc",
    "word",
    lambda: [Word(word="apple"), Word(word="BANANA"), Word(word="cat")],
)
