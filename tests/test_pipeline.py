from typing import Generator, AsyncGenerator
import pytest
import itertools
from databeakers.pipeline import Pipeline, ErrorType
from databeakers.exceptions import InvalidGraph
from databeakers.edges import Transform
from databeakers._models import RunMode
from examples import Word, Sentence, fruits


def capitalized(word: Word) -> Word:
    return Word(word=word.word.capitalize())  # type: ignore


@pytest.fixture
def wc_pipeline():
    """simple fixture with just beakers"""
    r = Pipeline("word_capitalized", ":memory:")
    r.add_beaker("word", Word)
    r.add_beaker("capitalized", Word)
    return r


def test_pipeline_repr() -> None:
    pipeline = Pipeline("test")
    assert repr(pipeline) == "Pipeline(test)"


def test_add_beaker_simple() -> None:
    pipeline = Pipeline("test")
    pipeline.add_beaker("word", Word)
    assert pipeline.beakers["word"].name == "word"
    assert pipeline.beakers["word"].model == Word
    assert pipeline.beakers["word"].pipeline == pipeline


def test_add_transform(wc_pipeline):
    wc_pipeline.add_transform("word", "capitalized", capitalized)
    assert wc_pipeline.graph["word"]["capitalized"]["edge"] == Transform(
        name="capitalized",
        func=capitalized,
        error_map={},
        whole_record=False,
        allow_filter=True,
    )


def test_add_transform_lambda(wc_pipeline):
    wc_pipeline.add_transform("word", "capitalized", lambda x: x)
    assert wc_pipeline.graph["word"]["capitalized"]["edge"].name == "λ"


def test_add_transform_error_map(wc_pipeline):
    wc_pipeline.add_transform(
        "word", "capitalized", capitalized, error_map={(ValueError,): "error"}
    )
    assert wc_pipeline.graph["word"]["capitalized"]["edge"].error_map == {
        (ValueError,): "error"
    }


def test_add_transform_bad_from_beaker():
    pipeline = Pipeline("test")
    pipeline.add_beaker("capitalized", Word)
    with pytest.raises(InvalidGraph):
        pipeline.add_transform("word", "capitalized", capitalized)


def test_add_transform_implicit_error_beaker(wc_pipeline):
    wc_pipeline.add_transform(
        "word", "capitalized", capitalized, error_map={(Exception,): "error"}
    )
    # error beaker is created implicitly if it didn't exist
    assert wc_pipeline.beakers["error"].model == ErrorType
    # TODO: assert that warning was logged


def test_add_transform_bad_annotation_parameter(wc_pipeline):
    def bad_annotation(_: int) -> Word:
        return Word(word="apple")

    def bad_annotation_ret(_: Word) -> int:
        return 7

    with pytest.raises(InvalidGraph) as e:
        wc_pipeline.add_transform("word", "capitalized", bad_annotation)
    assert "expects int, word contains Word" in str(e)
    with pytest.raises(InvalidGraph) as e:
        wc_pipeline.add_transform("word", "capitalized", bad_annotation_ret)
    assert "returns int, capitalized expects Word" in str(e)


def test_add_transform_implicit_return_beaker():
    """test that if return beaker is not specified and does not exist, it is created implicitly"""
    p = Pipeline("test", ":memory:")
    p.add_beaker("word", Word)
    p.add_transform("word", "capitalized", capitalized)
    assert p.beakers["capitalized"].model == Word


def test_add_transform_no_implicit_return_beaker():
    """if there is no annotation on the return type, we can't create a beaker"""
    pipeline = Pipeline("test")
    pipeline.add_beaker("word", Word)
    with pytest.raises(InvalidGraph):
        pipeline.add_transform("word", "capitalized", lambda x: x.upper())


def test_add_transform_bad_error_beaker_type(wc_pipeline):
    wc_pipeline.add_beaker("error", Word)
    with pytest.raises(InvalidGraph) as e:
        wc_pipeline.add_transform(
            "word",
            "capitalized",
            lambda x: x.upper(),
            error_map={(Exception,): "error"},
        )
    assert "Error beaker 'error' must use beakers.pipeline.ErrorType" in str(e)


def test_graph_data_simple():
    r = Pipeline("test")
    r.add_beaker("word", Word)
    r.add_beaker("capitalized", Word)
    r.add_beaker("filtered", Word)
    r.add_transform("word", "capitalized", capitalized)
    r.add_transform(
        "capitalized", "filtered", lambda x: x if x.word.startswith("A") else None
    )
    gd = r.graph_data()
    assert len(gd) == 3
    assert gd[0]["len"] == 0
    assert gd[0]["name"] == "capitalized"
    assert gd[0]["temp"] is False
    assert gd[0]["edges"][0]["to_beaker"] == "filtered"
    assert gd[0]["edges"][0]["edge"].name == "λ"
    assert gd[1] == {
        "len": 0,
        "name": "filtered",
        "temp": False,
        "edges": [],
    }
    assert gd[2] == {
        "len": 0,
        "name": "word",
        "temp": False,
        "edges": [
            {
                "to_beaker": "capitalized",
                "edge": Transform(
                    name="capitalized",
                    func=capitalized,
                    error_map={},
                    whole_record=False,
                    allow_filter=True,
                ),
            }
        ],
    }


@pytest.mark.parametrize("mode", [RunMode.waterfall, RunMode.river])
def test_run_fruits(mode):
    fruits.reset()
    fruits.run_seed("abc")
    assert len(fruits.beakers["word"]) == 3
    report = fruits.run(mode)

    assert report.only_beakers == []
    assert report.start_time is not None
    assert report.end_time is not None

    assert report.nodes["word"]["_already_processed"] == 0
    assert report.nodes["word"]["normalized"] == 3
    assert report.nodes["normalized"]["fruit"] == 2
    assert report.nodes["fruit"]["sentence"] == 2

    assert len(fruits.beakers["normalized"]) == 3
    assert len(fruits.beakers["fruit"]) == 2
    assert len(fruits.beakers["sentence"]) == 2

    sentences = sorted(
        [
            fruits.beakers["sentence"].get_item(id).sentence
            for id in fruits.beakers["sentence"].id_set()
        ]
    )

    assert sentences == [
        "apple is a delicious fruit.",
        "banana is a delicious fruit.",
    ]


@pytest.mark.parametrize("mode", [RunMode.waterfall, RunMode.river])
def test_run_early_end(mode):
    fruits.reset()
    fruits.run_seed("abc")
    assert len(fruits.beakers["word"]) == 3
    report = fruits.run(mode, only_beakers=["word", "normalized"])

    assert report.only_beakers == ["word", "normalized"]

    assert report.nodes["word"]["_already_processed"] == 0
    assert report.nodes["word"]["normalized"] == 3
    assert report.nodes["normalized"]["fruit"] == 2
    assert "fruit" not in report.nodes

    assert len(fruits.beakers["normalized"]) == 3
    assert len(fruits.beakers["fruit"]) == 2
    assert len(fruits.beakers["sentence"]) == 0


@pytest.mark.parametrize("mode", [RunMode.waterfall, RunMode.river])
def test_run_late_start(mode):
    fruits.reset()
    fruits.add_seed(
        "prenormalized",
        "normalized",
        lambda: [
            Word(word="apple"),
            Word(word="pear"),
            Word(word="banana"),
            Word(word="egg"),
            Word(word="fish"),
        ],
    )
    fruits.run_seed("prenormalized")
    assert len(fruits.beakers["word"]) == 0
    assert len(fruits.beakers["normalized"]) == 5
    report = fruits.run(mode, only_beakers=["normalized", "fruit", "sentence"])

    assert report.only_beakers == ["normalized", "fruit", "sentence"]

    assert "word" not in report.nodes
    assert report.nodes["normalized"]["_already_processed"] == 0
    assert report.nodes["normalized"]["fruit"] == 3
    assert report.nodes["fruit"]["sentence"] == 3

    assert len(fruits.beakers["normalized"]) == 5
    assert len(fruits.beakers["fruit"]) == 3
    assert len(fruits.beakers["sentence"]) == 3


@pytest.mark.parametrize("mode", [RunMode.waterfall, RunMode.river])
def test_run_twice(mode):
    fruits.reset()
    fruits.run_seed("abc")
    assert len(fruits.beakers["word"]) == 3
    fruits.run(mode)
    assert len(fruits.beakers["normalized"]) == 3
    assert len(fruits.beakers["fruit"]) == 2
    second_report = fruits.run(mode)

    assert second_report.nodes["word"]["_already_processed"] == 3
    # TODO: this should be three, since the first run should have
    #      processed all three items, but it's two because the second run
    #      doesn't know about the items rejected by the filter.
    # TODO  even worse with river mode
    # assert second_report.nodes["normalized"]["_already_processed"] == 2
    assert second_report.nodes["normalized"]["fruit"] == 0

    assert len(fruits.beakers["normalized"]) == 3
    assert len(fruits.beakers["fruit"]) == 2


@pytest.mark.parametrize("mode", [RunMode.waterfall, RunMode.river])
def test_run_errormap(mode):
    fruits.reset()
    fruits.run_seed("errors")  # [100, "pear", "ERROR"]
    assert len(fruits.beakers["word"]) == 3
    report = fruits.run(mode)

    # 100 winds up in non-words, two go on
    assert report.nodes["word"]["_already_processed"] == 0
    assert report.nodes["word"]["normalized"] == 2
    assert report.nodes["word"]["nonword"] == 1
    assert len(fruits.beakers["nonword"]) == 1
    assert len(fruits.beakers["normalized"]) == 2

    # ERROR winds up in errors, one goes on
    assert report.nodes["normalized"]["errors"] == 1
    assert report.nodes["normalized"]["fruit"] == 1
    assert len(fruits.beakers["errors"]) == 1
    assert len(fruits.beakers["fruit"]) == 1


@pytest.mark.parametrize("mode", [RunMode.waterfall, RunMode.river])
def test_run_errormap_twice(mode):
    # this test ensures that things that error out aren't processed twice
    fruits.reset()
    fruits.run_seed("errors")  # [100, "pear", "ERROR"]

    # processed once
    fruits.run(mode)
    assert len(fruits.beakers["nonword"]) == 1
    assert len(fruits.beakers["errors"]) == 1
    assert len(fruits.beakers["fruit"]) == 1

    report = fruits.run(mode)
    assert report.nodes["word"]["_already_processed"] == 3


@pytest.mark.parametrize("mode", [RunMode.waterfall, RunMode.river])
def test_run_error_out(mode):
    fruits.reset()

    # raise a zero division error, unhandled
    fruits.beakers["word"].add_item(Word(word="/0"), parent=None)

    # uncaught error from is_fruit, propagates
    with pytest.raises(ZeroDivisionError):
        fruits.run(mode)


@pytest.mark.parametrize("mode", [RunMode.waterfall, RunMode.river])
def test_run_async_functions_in_pipeline(mode):
    async def sentence_maker(word: Word) -> Sentence:
        return Sentence(sentence=f"{word.word} was processed asynchronously.")

    def words_seed() -> Generator[Word, None, None]:
        for n in range(10):
            yield from [
                Word(word=f"up {n}"),
                Word(word=f"down {n}"),
                Word(word=f"strange {n}"),
                Word(word=f"charm {n}"),
                Word(word=f"top {n}"),
                Word(word=f"bottom {n}"),
            ]

    async_test = Pipeline("async_test", "async_test.db")
    async_test.add_beaker("word", Word)
    async_test.add_beaker("sentence", Sentence)
    async_test.add_transform(
        "word",
        "sentence",
        sentence_maker,
    )
    async_test.add_seed("words", "word", words_seed)

    async_test.reset()
    async_test.run_seed("words")
    assert len(async_test.beakers["word"]) == 60
    report = async_test.run(mode)

    assert report.nodes["word"]["_already_processed"] == 0
    assert report.nodes["word"]["sentence"] == 60

    assert len(async_test.beakers["sentence"]) == 60

    sentences = [
        async_test.beakers["sentence"].get_item(id).sentence
        for id in async_test.beakers["sentence"].id_set()
    ]

    assert len(sentences) == 60


@pytest.mark.parametrize("mode", [RunMode.waterfall, RunMode.river])
def test_run_generator_func(mode):
    def anagrams(word: Word) -> Generator[Word, None, None]:
        for perm in itertools.permutations(str(word.word)):
            yield Word(word="".join(perm))

    def words_seed() -> Generator[Word, None, None]:
        yield from [
            Word(word="cat"),
            Word(word="dog"),
        ]

    p = Pipeline("test", ":memory:")
    p.add_beaker("word", Word)
    p.add_beaker("anagram", Word)
    p.add_transform("word", "anagram", anagrams)
    p.add_seed("words", "word", words_seed)

    p.run_seed("words")
    assert len(p.beakers["word"]) == 2
    report = p.run(mode)

    assert report.nodes["word"]["_already_processed"] == 0
    assert report.nodes["word"]["anagram"] == 2  # two moved from word -> anagram
    assert len(p.beakers["anagram"]) == 12  # but 12 were created


@pytest.mark.parametrize("mode", [RunMode.waterfall, RunMode.river])
def test_run_async_generator_func(mode):
    async def anagrams(word: Word) -> AsyncGenerator[Word, None]:
        for perm in itertools.permutations(str(word.word)):
            yield Word(word="".join(perm))

    def words_seed() -> Generator[Word, None, None]:
        yield from [
            Word(word="cat"),
            Word(word="dog"),
        ]

    p = Pipeline("test", ":memory:")
    p.add_beaker("word", Word)
    p.add_beaker("anagram", Word)
    p.add_transform("word", "anagram", anagrams)
    p.add_seed("words", "word", words_seed)

    p.run_seed("words")
    assert len(p.beakers["word"]) == 2
    report = p.run(mode)

    assert report.nodes["word"]["_already_processed"] == 0
    assert report.nodes["word"]["anagram"] == 2  # two moved from word -> anagram
    assert len(p.beakers["anagram"]) == 12  # but 12 were created
