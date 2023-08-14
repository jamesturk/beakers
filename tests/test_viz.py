from databeakers.pipeline import Pipeline
from databeakers.beakers import TempBeaker
from examples import Word


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
    assert b"word -> error\t[color=red," in dot
    assert b"word -> zero_division\t[color=red," in dot
