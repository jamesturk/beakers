import pytest
from examples import Word
from databeakers.pipeline import Pipeline, RunMode
from databeakers.edges import Splitter, Transform

animals = ["dog", "cat", "bird", "fish"]
minerals = ["gold", "silver", "copper", "iron", "lead", "tin", "zinc"]
cryptids = ["bigfoot"]


def splitter_func(word: Word):
    if word.word in animals:
        return "animal"
    elif word.word in minerals:
        return "mineral"
    elif word.word in cryptids:
        return "cryptid"
    return None


@pytest.fixture
def pipeline():
    p = Pipeline("splitter", ":memory:")
    p.add_beaker("word", Word)
    p.add_beaker("cryptid", Word)
    p.add_beaker("animal", Word)
    p.add_beaker("mineral", Word)

    animal_t = Transform(
        func=lambda x: Word(word="you can get a pet " + x.word),
        to_beaker="animal",
    )
    mineral_t = Transform(
        func=lambda x: Word(word="i found some " + x.word),
        to_beaker="mineral",
    )
    cryptid_t = Transform(
        func=lambda x: Word(word="have you seen a " + x.word),
        to_beaker="cryptid",
    )

    p.add_splitter(
        "word",
        Splitter(
            func=splitter_func,
            splitter_map={
                "animal": animal_t,
                "mineral": mineral_t,
                "cryptid": cryptid_t,
            },
        ),
    )

    return p


@pytest.mark.parametrize("run_mode", [RunMode.waterfall, RunMode.river])
def test_splitter(pipeline, run_mode):
    for word in animals + minerals + cryptids:
        pipeline.beakers["word"].add_item(Word(word=word), parent=None)

    result = pipeline.run(run_mode=run_mode)

    assert result.nodes["word"]["mineral"] == 7
    assert result.nodes["word"]["animal"] == 4
    assert result.nodes["word"]["cryptid"] == 1

    assert len(pipeline.beakers["word"]) == 12
    assert len(pipeline.beakers["mineral"]) == 7
    assert len(pipeline.beakers["animal"]) == 4
    assert len(pipeline.beakers["cryptid"]) == 1
