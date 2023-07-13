import uuid
from pydantic import BaseModel


class Record:
    _reserved_names = ("id",)

    def __init__(self, id: str | None = None):
        self._id = id if id else str(uuid.uuid1())
        self._data: dict[str, BaseModel] = {}

    def __getattr__(self, name: str) -> str | BaseModel:
        if name == "id":
            return self._id
        return self._data[name]

    def __setattr__(self, name: str, value: BaseModel) -> None:
        if name.startswith("_"):
            super().__setattr__(name, value)
        elif name not in self._data and name not in self._reserved_names:
            self._data[name] = value
        else:
            raise AttributeError(f"DataObject attribute {name} already exists")
