import pytest
import pathlib
from pydantic import BaseModel
from databeakers.beakers import DirectoryBeaker


class DataModel(BaseModel):
    data: str

    def write_to_path(self, path):
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path.with_suffix(".txt"), "w") as f:
            f.write(self.data)


@pytest.fixture
def dirb():
    # rm -rf _files/test
    dirname = pathlib.Path("_files/test")
    for pd in dirname.iterdir():
        for f in pd.iterdir():
            f.unlink()
        pd.rmdir()
    pathlib.Path("_files/test").mkdir(parents=True, exist_ok=True)
    return DirectoryBeaker("test", DataModel, None)


def test_dir_beaker_empty(dirb):
    assert len(dirb) == 0
    assert dirb.parent_id_set() == set()


def test_dir_beaker_add_item(dirb):
    dirb.add_item(DataModel(data="test"), parent="abc", id_="1")
    assert len(dirb) == 1
    assert dirb.parent_id_set() == {"abc"}

    with open("_files/test/abc/1.txt", "r") as f:
        assert f.read() == "test"
