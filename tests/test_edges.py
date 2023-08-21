import pytest
from examples import Word
from pydantic import BaseModel
from databeakers.pipeline import Pipeline, RunMode
from databeakers.edges import Splitter, Transform, FieldSplitter, Conditional
from databeakers.exceptions import BadSplitResult

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
def test_splitter_in_pipeline(pipeline, run_mode):
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


@pytest.mark.asyncio
async def test_splitter():
    splitter = Splitter(
        lambda x: x.word[0].lower(),
        splitter_map={
            "a": Transform(lambda x: Word(word=x.word.upper()), to_beaker="upper"),
            "b": Transform(lambda x: Word(word=x.word.lower()), to_beaker="lower"),
        },
    )

    # probably a better way to do this?
    async for res in splitter._run("1", Word(word="Apple")):
        pass
    assert res.dest == "upper"
    assert res.data.word == "APPLE"
    async for res in splitter._run("1", Word(word="bAnAnA")):
        pass
    assert res.dest == "lower"
    assert res.data.word == "banana"


class Tagged(BaseModel):
    tag: str
    word: str


@pytest.mark.asyncio
async def test_splitter_bad_result():
    splitter = Splitter(
        lambda x: x.word[0].lower(),
        splitter_map={
            "a": Transform(lambda x: Word(word=x.word.upper()), to_beaker="upper"),
            "b": Transform(lambda x: Word(word=x.word.lower()), to_beaker="lower"),
        },
    )

    with pytest.raises(BadSplitResult):
        async for _ in splitter._run("1", Word(word="Chip")):
            pass


@pytest.mark.asyncio
async def test_field_splitter():
    splitter = FieldSplitter(
        "tag",
        splitter_map={
            "a": Transform(lambda x: Word(word=x.word.upper()), to_beaker="upper"),
            "b": Transform(lambda x: Word(word=x.word.lower()), to_beaker="lower"),
        },
    )

    # probably a better way to do this?
    async for res in splitter._run("1", Tagged(tag="a", word="Apple")):
        pass
    assert res.dest == "upper"
    assert res.data.word == "APPLE"
    async for res in splitter._run("1", Tagged(tag="b", word="bAnAnA")):
        pass
    assert res.dest == "lower"
    assert res.data.word == "banana"


@pytest.mark.asyncio
async def test_field_splitter_whole_record():
    # mock a Record as a dict w/ tb beaker as only  key
    splitter = FieldSplitter(
        "tag",
        beaker_name="tb",
        whole_record=True,
        splitter_map={
            "a": Transform(
                lambda x: Word(word=x["tb"].word.upper()), to_beaker="upper"
            ),
            "b": Transform(
                lambda x: Word(word=x["tb"].word.lower()), to_beaker="lower"
            ),
        },
    )

    # probably a better way to do this?
    async for res in splitter._run("1", {"tb": Tagged(tag="a", word="Apple")}):
        pass
    assert res.dest == "upper"
    assert res.data.word == "APPLE"
    async for res in splitter._run("1", {"tb": Tagged(tag="b", word="bAnAnA")}):
        pass
    assert res.dest == "lower"
    assert res.data.word == "banana"


@pytest.mark.asyncio
async def test_field_splitter_beaker_name_no_whole_record():
    # invalid state - whole_record and beaker_name must be specified together
    with pytest.raises(ValueError):
        FieldSplitter(
            "tag",
            whole_record=True,
            splitter_map={
                "a": Transform(
                    lambda x: Word(word=x["tb"].word.upper()), to_beaker="upper"
                ),
                "b": Transform(
                    lambda x: Word(word=x["tb"].word.lower()), to_beaker="lower"
                ),
            },
        )
    with pytest.raises(ValueError):
        FieldSplitter(
            "tag",
            beaker_name="tb",
            splitter_map={
                "a": Transform(
                    lambda x: Word(word=x["tb"].word.upper()), to_beaker="upper"
                ),
                "b": Transform(
                    lambda x: Word(word=x["tb"].word.lower()), to_beaker="lower"
                ),
            },
        )


@pytest.mark.asyncio
async def test_conditional_splitter():
    splitter = Conditional(
        condition=lambda x: bool(x.tag),
        if_true=Transform(lambda x: Word(word=x.word.upper()), to_beaker="upper"),
        if_false=Transform(lambda x: Word(word=x.word.lower()), to_beaker="lower"),
    )

    # probably a better way to do this?
    async for res in splitter._run("1", Tagged(tag="exists", word="Apple")):
        pass
    assert res.dest == "upper"
    assert res.data.word == "APPLE"
    async for res in splitter._run("1", Tagged(tag="", word="bAnAnA")):
        pass
    assert res.dest == "lower"
    assert res.data.word == "banana"
