import abc
import json
import uuid
from pydantic import BaseModel
from typing import Iterable, Type, TYPE_CHECKING
from structlog import get_logger
from sqlite_utils.db import NotFoundError
from .exceptions import ItemNotFound

if TYPE_CHECKING:  # pragma: no cover
    from .pipeline import Pipeline

PydanticModel = Type[BaseModel]

log = get_logger()


class Beaker(abc.ABC):
    def __init__(self, name: str, model: PydanticModel, pipeline: "Pipeline"):
        self.name = name
        self.model = model
        self.pipeline = pipeline

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name}, {self.model.__name__})"

    @abc.abstractmethod
    def __len__(self) -> int:
        """
        Return number of items in the beaker.
        """

    @abc.abstractmethod
    def all_ids(self, ordered: bool = False) -> Iterable[str]:
        """
        Return set of ids.
        """

    @abc.abstractmethod
    def parent_id_set(self) -> set[str]:
        """
        Return set of parent ids.
        """

    @abc.abstractmethod
    def items(self) -> Iterable[tuple[str, BaseModel]]:
        """
        Return iterable of items in the beaker.
        """

    @abc.abstractmethod
    def get_item(self, id: str) -> BaseModel:
        """
        Get an item from the beaker by id.
        """

    @abc.abstractmethod
    def add_item(
        self, item: BaseModel, *, parent: str | None, id_: str | None = None
    ) -> None:
        """
        Add an item to the beaker.
        """

    @abc.abstractmethod
    def delete(self, parent: str) -> int:
        """
        Delete all items with the given parent id.

        Return number of items deleted.
        """
        return 0

    @abc.abstractmethod
    def reset(self) -> None:
        """
        Reset the beaker to empty.
        """


class TempBeaker(Beaker):
    def __init__(self, name: str, model: PydanticModel, pipeline: "Pipeline"):
        super().__init__(name, model, pipeline)
        self._items: dict[str, BaseModel] = {}
        self._parent_ids: dict[str, str] = {}  # map id to parent id

    def __len__(self) -> int:
        return len(self._items)

    def parent_id_set(self) -> set[str]:
        return set(self._parent_ids.values())

    def all_ids(self, ordered: bool = False) -> Iterable[str]:
        ids: Iterable[str] = self._items.keys()
        if ordered:
            ids = sorted(ids)
        return ids

    def add_item(
        self, item: BaseModel, *, parent: str | None, id_: str | None = None
    ) -> None:
        if parent is None:
            parent = id_ = str(uuid.uuid1())
        if id_ is None:
            id_ = str(uuid.uuid1())
        log.debug("add_item", item=item, parent=parent, id=id_, beaker=self.name)
        self._items[id_] = item
        self._parent_ids[id_] = parent

    def items(self) -> Iterable[tuple[str, BaseModel]]:
        yield from self._items.items()

    def reset(self) -> None:
        self._items = {}
        self._parent_ids = {}

    def get_item(self, id: str) -> BaseModel:
        try:
            return self._items[id]
        except KeyError:
            raise ItemNotFound(f"{id} not found in {self.name}")

    def delete(self, parent: str) -> int:
        deleted = 0
        for id, parent_id in self._parent_ids.items():
            if parent_id == parent:
                del self._items[id]
                del self._parent_ids[id]
                deleted += 1
        return deleted


class SqliteBeaker(Beaker):
    def __init__(self, name: str, model: PydanticModel, pipeline: "Pipeline"):
        super().__init__(name, model, pipeline)
        # create table if it doesn't exist
        self._table = self.pipeline._db[name].create(
            {
                "uuid": str,
                "parent": str,
                "data": dict,  # JSON
            },
            pk="uuid",
            if_not_exists=True,
        )
        log.debug("beaker initialized", count=self._table.count, name=self.name)
        # TODO: allow pydantic-to-model here

    def __len__(self) -> int:
        return self._table.count

    def parent_id_set(self) -> set[str]:
        return {row["parent"] for row in self._table.rows}

    def all_ids(self, ordered: bool = False) -> Iterable[str]:
        ids = [row["uuid"] for row in self._table.rows]
        if ordered:
            ids = sorted(ids)
        return ids

    def items(self) -> Iterable[tuple[str, BaseModel]]:
        for item in self._table.rows:
            yield item["uuid"], self.model(**json.loads(item["data"]))

    def add_item(
        self, item: BaseModel, *, parent: str | None, id_: str | None = None
    ) -> None:
        if not hasattr(item, "model_dump_json"):
            raise TypeError(
                f"beaker {self.name} received {item!r} ({type(item)}), "
                f"expecting an instance of {self.model}"
            )
        if parent is None:
            parent = id_ = str(uuid.uuid1())
        elif id_ is None:
            id_ = str(uuid.uuid1())
        log.debug(
            "add_item",
            item=item,
            parent=parent,
            id=id_,
            beaker=self.name,
        )
        self._table.db.execute(
            f"INSERT INTO {self.name} (uuid, parent, data) VALUES (?, ?, ?)",
            (id_, parent, item.model_dump_json()),
        )

    def get_item(self, id: str) -> BaseModel:
        try:
            row = self._table.get(id)
        except NotFoundError:
            raise ItemNotFound(f"{id} not found in {self.name}")
        return self.model(**json.loads(row["data"]))

    def reset(self) -> None:
        log.info("beaker cleared", beaker=self.name)
        self._table.delete_where()

    def delete(self, parent: str) -> int:
        before = self._table.count
        self._table.delete_where("parent=?", (parent,))
        after = self._table.count
        log.info(
            "beaker delete where",
            beaker=self.name,
            parent=parent,
            deleted=before - after,
        )
        return before - after
