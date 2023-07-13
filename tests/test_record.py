from beakers.record import Record
import pytest


def test_record_id_autogen():
    r = Record()
    assert len(r.id) == 36
    r2, r3 = Record(), Record()
    assert r2.id != r3.id


def test_record_id_assign():
    r = Record(id="test")
    assert r.id == "test"


def test_record_setattr_good():
    r = Record()
    r.attrib = "set"
    assert r.attrib == "set"


def test_record_setattr_duplicate():
    r = Record()
    r.attrib = "set"
    with pytest.raises(AttributeError):
        r.attrib = "changed"
    assert r.attrib == "set"


def test_record_setattr_id():
    r = Record()
    with pytest.raises(AttributeError):
        r.id = "changed"
