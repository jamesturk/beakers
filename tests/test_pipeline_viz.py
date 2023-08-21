from databeakers.pipeline import Pipeline
from databeakers.beakers import TempBeaker
from examples import Word
from test_edges import pipeline as splitter_pipeline  # noqa


def test_basic_graph():
    p = Pipeline("word_capitalized", ":memory:")
    p.add_beaker("word", Word)
    p.add_beaker("capitalized", Word)
    p.add_transform("word", "capitalized", lambda x: x.upper())

    dot = p.to_pydot().create_dot()
    assert b"word\t[color=blue," in dot
    assert b"capitalized\t[color=blue," in dot
    assert b"word -> capitalized" in dot


def test_graph_temp_node():
    p = Pipeline("word_capitalized", ":memory:")

    p.add_beaker("word", Word)
    p.add_beaker("capitalized", Word, beaker_type=TempBeaker)
    p.add_transform("word", "capitalized", lambda x: x.upper())

    dot = p.to_pydot().create_dot()
    assert b"word\t[color=blue," in dot
    assert b"capitalized\t[color=grey," in dot
    assert b"word -> capitalized" in dot


def test_graph_error_nodes():
    p = Pipeline("word_capitalized", ":memory:")

    p.add_beaker("word", Word)
    p.add_beaker("capitalized", Word, beaker_type=TempBeaker)
    p.add_transform(
        "word",
        "capitalized",
        lambda x: x.upper(),
        error_map={
            (ValueError,): "error",
            (ZeroDivisionError,): "zero_division",
        },
    )

    dot = p.to_pydot().create_dot()
    # error nodes
    assert b"\terror\t[color=red," in dot
    assert b"\tzero_division\t[color=red," in dot
    # error lines
    print(dot)
    assert b'"word -> capitalized" -> error\t[color=red,' in dot
    assert b'"word -> capitalized" -> zero_division\t[color=red,' in dot


def test_graph_splitter(splitter_pipeline):  # noqa
    dot = splitter_pipeline.to_pydot().create_dot()
    assert b"word\t[color=blue," in dot
    assert b"animal\t[color=blue," in dot
    assert b"mineral\t[color=blue," in dot
    assert b"cryptid\t[color=blue," in dot
    assert b"splitter_func\t[color=green" in dot
    assert b"word -> splitter_func" in dot
    assert b"word -> splitter_func" in dot
    assert b"word -> splitter_func" in dot
    assert b"splitter_func -> animal" in dot
    assert b"splitter_func -> mineral" in dot
    assert b"splitter_func -> cryptid" in dot
