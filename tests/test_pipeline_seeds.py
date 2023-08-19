import pytest
import itertools
from databeakers.pipeline import Pipeline
from databeakers.exceptions import SeedError
from examples import Word


def places():
    yield Word(word="north carolina")
    yield Word(word="new york")
    yield Word(word="montana")
    yield Word(word="washington dc")
    yield Word(word="illinois")


def farm():
    yield Word(word="cow")
    yield Word(word="pig")
    yield Word(word="chicken")
    yield Word(word="horse")
    yield Word(word="goat")


def zoo():
    yield Word(word="lion")
    yield Word(word="tiger")
    yield Word(word="bear")
    yield Word(word="elephant")
    yield Word(word="giraffe")
    yield Word(word="zebra")
    yield Word(word="monkey")
    yield Word(word="gorilla")
    yield Word(word="penguin")


@pytest.fixture
def pipeline():
    p = Pipeline("seeds", ":memory:")
    p.add_beaker("animal", Word)
    p.add_beaker("place", Word)
    p.register_seed(places, "place")
    p.register_seed(farm, "animal")
    p.register_seed(zoo, "animal")
    return p


def anagrams(word):
    for anagram in itertools.permutations(word):
        yield Word(word="".join(anagram))


def combined(word1, word2):
    yield Word(word=f"{word1} {word2}")


@pytest.fixture
def anagram_p():
    p = Pipeline("seeds", ":memory:")
    p.add_beaker("word", Word)
    p.register_seed(anagrams, "word")
    return p


def test_list_seeds_empty(pipeline):
    res = pipeline.list_seeds()
    assert res == {"animal": {"farm": [], "zoo": []}, "place": {"places": []}}


def test_list_seeds_runs(pipeline):
    pipeline.run_seed("places")
    res = pipeline.list_seeds()
    run = res["place"]["places"][0]
    assert run.num_items == 5
    assert run.run_repr == "sr:places"


def test_list_seeds_multiple_runs(anagram_p):
    anagram_p.run_seed("anagrams", parameters={"word": "test"})
    anagram_p.run_seed("anagrams", parameters={"word": "wow"})
    res = anagram_p.list_seeds()
    assert len(res["word"]["anagrams(word)"]) == 2
    test, wow = res["word"]["anagrams(word)"]
    assert test.num_items == 24
    assert test.run_repr == "sr:anagrams[word=test]"
    assert wow.num_items == 6
    assert wow.run_repr == "sr:anagrams[word=wow]"


def test_list_seeds_multiple_runs_multiple_parameters(anagram_p):
    anagram_p.register_seed(combined, "word")
    anagram_p.run_seed("combined", parameters={"word1": "oh", "word2": "wow"})
    anagram_p.run_seed("combined", parameters={"word2": "buddy", "word1": "hey"})
    assert len(anagram_p.list_seeds()["word"]["combined(word1, word2)"]) == 2
    oh_wow, hey_buddy = anagram_p.list_seeds()["word"]["combined(word1, word2)"]
    assert oh_wow.num_items == 1
    assert oh_wow.run_repr == "sr:combined[word1=oh,word2=wow]"
    assert hey_buddy.num_items == 1
    assert hey_buddy.run_repr == "sr:combined[word1=hey,word2=buddy]"


def test_run_seed(pipeline):
    pipeline.run_seed("places")

    assert len(pipeline.beakers["place"]) == 5
    assert pipeline.get_seed_run("sr:places") is not None


@pytest.fixture
def error_pipeline():
    def error_seed():
        for i in range(90):
            yield Word(word="test")
        raise ZeroDivisionError("ZeroDivisionError")

    p = Pipeline("seeds", ":memory:")
    p.add_beaker("words", Word)
    p.register_seed(error_seed, "words")
    return p


def test_run_seed_no_save_bad_runs(error_pipeline):
    # set low chunk size, to ensure deletion is required
    res = error_pipeline.run_seed("error_seed", save_bad_runs=False, chunk_size=10)
    assert len(error_pipeline.beakers["words"]) == 0
    assert error_pipeline.get_seed_run("sr:error_seed") is None  # not saved to db
    assert res.num_items == 0
    assert res.error == "ZeroDivisionError"


def test_run_seed_partial_chunks_saved(error_pipeline):
    res = error_pipeline.run_seed("error_seed", chunk_size=60)
    assert len(error_pipeline.beakers["words"]) == 60
    assert res.num_items == 60
    assert res.error == "ZeroDivisionError"
    # saved to db
    assert error_pipeline.get_seed_run("sr:error_seed") == res


def test_run_seed_partial_chunks_nothing_saved(error_pipeline):
    # too big of a chunk size, error is raised before commit
    res = error_pipeline.run_seed("error_seed", chunk_size=100)
    assert len(error_pipeline.beakers["words"]) == 0
    assert res.num_items == 0
    assert res.error == "ZeroDivisionError"
    # saved to db
    assert error_pipeline.get_seed_run("sr:error_seed") == res


def test_run_two_seeds(pipeline):
    pipeline.run_seed("farm")
    pipeline.run_seed("zoo")

    assert len(pipeline.beakers["animal"]) == 14


def test_run_seed_bad_name(pipeline):
    with pytest.raises(SeedError):
        pipeline.run_seed("bad")


def test_run_seed_already_run(pipeline):
    pipeline.run_seed("farm")
    with pytest.raises(SeedError):
        pipeline.run_seed("farm")
    assert len(pipeline.beakers["animal"]) == 5


def test_reset_all_resets_seeds(pipeline):
    pipeline.run_seed("farm")
    pipeline.run_seed("zoo")
    assert len(pipeline.beakers["animal"]) == 14
    pipeline.reset()
    assert len(pipeline.beakers["animal"]) == 0


def test_run_seed_limit(pipeline):
    pipeline.run_seed("zoo", max_items=2)
    assert len(pipeline.beakers["animal"]) == 2


def test_run_seed_reset(pipeline):
    pipeline.run_seed("farm")
    assert len(pipeline.beakers["animal"]) == 5
    pipeline.run_seed("farm", reset=True)
    assert len(pipeline.beakers["animal"]) == 5


def test_get_run(pipeline):
    pipeline.run_seed("places")

    run = pipeline.get_seed_run("sr:places")
    assert run.beaker_name == "place"
    assert run.seed_name == "places"
    assert run.run_repr == "sr:places"
    assert run.num_items == 5
    assert run.start_time is not None
    assert run.end_time is not None


def test_run_seed_parameters_basic(anagram_p):
    res = anagram_p.run_seed(
        "anagrams", parameters={"word": "test"}, save_bad_runs=False
    )

    assert anagram_p.get_seed_run("sr:anagrams") is None
    assert res == anagram_p.get_seed_run("sr:anagrams[word=test]")
    assert len(anagram_p.beakers["word"]) == 24


def test_run_seed_parameters_multiple_runs(anagram_p):
    res = anagram_p.run_seed(
        "anagrams", parameters={"word": "test"}, save_bad_runs=False
    )
    assert res == anagram_p.get_seed_run("sr:anagrams[word=test]")
    assert len(anagram_p.beakers["word"]) == 24

    # different parameters
    res = anagram_p.run_seed(
        "anagrams", parameters={"word": "cat"}, save_bad_runs=False
    )
    assert res == anagram_p.get_seed_run("sr:anagrams[word=cat]")
    assert len(anagram_p.beakers["word"]) == 24 + 6


def test_run_multiple_parameters_repr(anagram_p):
    # order should not matter
    anagram_p.register_seed(combined, "word")
    res = anagram_p.run_seed("combined", parameters={"word1": "oh", "word2": "wow"})
    assert res.run_repr == "sr:combined[word1=oh,word2=wow]"
    res = anagram_p.run_seed("combined", parameters={"word2": "buddy", "word1": "hey"})
    assert res.run_repr == "sr:combined[word1=hey,word2=buddy]"


def test_get_runs_multiple_parameters(anagram_p):
    anagram_p.run_seed("anagrams", parameters={"word": "test"}, save_bad_runs=False)
    anagram_p.run_seed("anagrams", parameters={"word": "cat"}, save_bad_runs=False)

    runs = anagram_p.get_seed_runs("anagrams")
    assert len(runs) == 2
    assert runs[0].run_repr == "sr:anagrams[word=test]"
    assert runs[1].run_repr == "sr:anagrams[word=cat]"


def test_run_seed_parameters_multiple_runs_identical(anagram_p):
    res1 = anagram_p.run_seed(
        "anagrams", parameters={"word": "test"}, save_bad_runs=False
    )

    # can't run same seed with same parameters
    with pytest.raises(SeedError):
        anagram_p.run_seed("anagrams", parameters={"word": "test"}, save_bad_runs=False)

    # record not updated
    assert res1 == anagram_p.get_seed_run("sr:anagrams[word=test]")
