import abc
import json
import sqlite3
import uuid
from pydantic import BaseModel
from typing import Iterable, Type, TYPE_CHECKING
from .exceptions import ItemNotFound

if TYPE_CHECKING:  # pragma: no cover
    from .recipe import Recipe

PydanticModel = Type[BaseModel]


class Beaker(abc.ABC):
    def __init__(self, name: str, model: PydanticModel, recipe: "Recipe"):
        self.name = name
        self.model = model
        self.recipe = recipe

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name}, {self.model.__name__})"

    @abc.abstractmethod
    def items(self) -> Iterable[tuple[str, BaseModel]]:
        """
        Return list of items in the beaker.
        """

    @abc.abstractmethod
    def __len__(self) -> int:
        """
        Return number of items in the beaker.
        """

    @abc.abstractmethod
    def add_item(self, item: BaseModel, id: str | None = None) -> None:
        """
        Add an item to the beaker, with an optional id.
        """

    @abc.abstractmethod
    def reset(self) -> None:
        """
        Reset the beaker to empty.
        """

    @abc.abstractmethod
    def get_item(self, id: str) -> BaseModel:
        """
        Get an item from the beaker by id.
        """

    def add_items(self, items: Iterable[BaseModel]) -> None:
        for item in items:
            self.add_item(item)

    def id_set(self) -> set[str]:
        return set(id for id, _ in self.items())


class TempBeaker(Beaker):
    def __init__(self, name: str, model: PydanticModel, recipe: "Recipe"):
        super().__init__(name, model, recipe)
        self._items: list[tuple[str, BaseModel]] = []

    def __len__(self) -> int:
        return len(self._items)

    def add_item(self, item: BaseModel, id: str | None = None) -> None:
        if id is None:
            id = str(uuid.uuid1())
        self._items.append((id, item))

    def items(self) -> Iterable[tuple[str, BaseModel]]:
        yield from self._items

    def reset(self) -> None:
        self._items = []

    def get_item(self, id: str) -> BaseModel:
        # TODO: make O(1)
        for item_id, item in self._items:
            if item_id == id:
                return item
        raise KeyError(f"{id} not found in {self.name}")


class SqliteBeaker(Beaker):
    def __init__(self, name: str, model: PydanticModel, recipe: "Recipe"):
        super().__init__(name, model, recipe)
        # create table if it doesn't exist
        self.cursor = self.recipe.db.cursor()
        self.cursor.row_factory = sqlite3.Row  # type: ignore
        self.cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {self.name} (uuid TEXT PRIMARY KEY, data JSON)"
        )

    def items(self) -> Iterable[tuple[str, BaseModel]]:
        self.cursor.execute(f"SELECT uuid, data FROM {self.name}")
        data = self.cursor.fetchall()
        for item in data:
            yield item["uuid"], self.model(**json.loads(item["data"]))

    def __len__(self) -> int:
        self.cursor.execute(f"SELECT COUNT(*) FROM {self.name}")
        return self.cursor.fetchone()[0]

    def add_item(self, item: BaseModel, id: str | None = None) -> None:
        if id is None:
            id = str(uuid.uuid1())
        self.cursor.execute(
            f"INSERT INTO {self.name} (uuid, data) VALUES (?, ?)",
            (id, item.model_dump_json()),
        )
        self.recipe.db.commit()

    def reset(self) -> None:
        self.cursor.execute(f"DELETE FROM {self.name}")
        self.recipe.db.commit()

    def get_item(self, id: str) -> BaseModel:
        self.cursor.execute(f"SELECT data FROM {self.name} WHERE uuid = ?", (id,))
        row = self.cursor.fetchone()
        if row is None:
            raise ItemNotFound(f"{id} not found in {self.name}")
        return self.model(**json.loads(row["data"]))
