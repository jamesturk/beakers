from databeakers._record import Record
import pytest


def test_record_setattr_good():
    r = Record(id="123")
    r.attrib = "set"
    assert r.attrib == "set"


def test_record_setattr_duplicate():
    r = Record(id="123")
    r["attrib"] = "set"
    with pytest.raises(AttributeError):
        r["attrib"] = "changed"
    assert r["attrib"] == "set"


def test_record_setattr_id():
    r = Record(id="123")
    with pytest.raises(AttributeError):
        r["id"] = "changed"


def test_record_getattr_id():
    r = Record(id="123")
    assert r["id"] == "123"


def test_record_repr():
    r = Record(id="123")
    assert repr(r) == "Record(123)"
