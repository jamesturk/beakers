import abc
import json
import uuid
from pydantic import BaseModel
from typing import Iterable, Type, TYPE_CHECKING
from structlog import get_logger
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
    def add_item(
        self, item: BaseModel, *, parent: str | None, id_: str | None = None
    ) -> None:
        """
        Add an item to the beaker.
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

    @abc.abstractmethod
    def parent_id_set(self) -> set[str]:
        """
        Return set of parent ids.
        """

    def id_set(self) -> set[str]:
        return set(id for id, _ in self.items())


class TempBeaker(Beaker):
    def __init__(self, name: str, model: PydanticModel, pipeline: "Pipeline"):
        super().__init__(name, model, pipeline)
        self._items: dict[str, BaseModel] = {}
        self._parent_ids: dict[str, str] = {}  # map id to parent id

    def __len__(self) -> int:
        return len(self._items)

    def add_item(
        self, item: BaseModel, *, parent: str | None, id_: str | None = None
    ) -> None:
        if parent is None:
            parent = id_ = str(uuid.uuid1())
        if id_ is None:
            id_ = str(uuid.uuid1())
        self._items[id_] = item
        self._parent_ids[id_] = parent

    def items(self) -> Iterable[tuple[str, BaseModel]]:
        yield from self._items.items()

    def reset(self) -> None:
        self._items = {}

    def get_item(self, id: str) -> BaseModel:
        try:
            return self._items[id]
        except KeyError:
            raise ItemNotFound(f"{id} not found in {self.name}")

    def parent_id_set(self) -> set[str]:
        return set(self._parent_ids.values())


class SqliteBeaker(Beaker):
    def __init__(self, name: str, model: PydanticModel, pipeline: "Pipeline"):
        super().__init__(name, model, pipeline)
        # create table if it doesn't exist
        self.pipeline.db.execute(
            f"CREATE TABLE IF NOT EXISTS {self.name} "
            "(uuid TEXT PRIMARY KEY, parent TEXT, data JSON)",
        )

    def items(self) -> Iterable[tuple[str, BaseModel]]:
        cursor = self.pipeline.db.execute(f"SELECT uuid, data FROM {self.name}")
        data = cursor.fetchall()
        for item in data:
            yield item["uuid"], self.model(**json.loads(item["data"]))

    def __len__(self) -> int:
        cursor = self.pipeline.db.execute(f"SELECT COUNT(*) FROM {self.name}")
        return cursor.fetchone()[0]

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
        log.debug("add_item", item=item, parent=parent, id=id_)
        self.pipeline.db.execute(
            f"INSERT INTO {self.name} (uuid, parent, data) VALUES (?, ?, ?)",
            (id_, parent, item.model_dump_json()),
        )

    def reset(self) -> None:
        self.pipeline.db.execute(f"DELETE FROM {self.name}")
        self.pipeline.db.commit()

    def get_item(self, id: str) -> BaseModel:
        cursor = self.pipeline.db.execute(
            f"SELECT data FROM {self.name} WHERE uuid = ?", (id,)
        )
        row = cursor.fetchone()
        if row is None:
            raise ItemNotFound(f"{id} not found in {self.name}")
        return self.model(**json.loads(row["data"]))

    def parent_id_set(self) -> set[str]:
        cursor = self.pipeline.db.execute(f"SELECT parent FROM {self.name}")
        return set(row["parent"] for row in cursor.fetchall())
