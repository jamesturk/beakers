from pydantic import BaseModel


class Record:
    """
    Internal class used to represent a record (sum of data)
    when moving through a pipeline.
    """

    _reserved_names = ("id",)

    def __init__(self, id: str):
        self.id = id
        self._data: dict[str, BaseModel] = {}

    def __getitem__(self, name: str) -> str | BaseModel:
        if name == "id":
            return self.id
        return self._data[name]

    def __setitem__(self, name: str, value: BaseModel) -> None:
        if name not in self._data and name not in self._reserved_names:
            self._data[name] = value
        else:
            raise AttributeError(f"DataObject attribute {name} already exists")

    def __contains__(self, name: str) -> bool:
        return name in self._data

    def __repr__(self):
        return f"{self.__class__.__name__}({self.id})"
