import abc
import json
import uuid
import pathlib
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
    def all_ids(
        self, ordered: bool = False, where: dict[str, str] | None = None
    ) -> Iterable[str]:
        """
        Return set of ids.
        """

    @abc.abstractmethod
    def all_ids_and_parents(self) -> Iterable[tuple[str, str]]:
        """
        Return iterable of (id, parent_id) tuples.
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
    def delete(
        self, *, parent: list[str] | None = None, ids: list[str] | None = None
    ) -> list[str]:
        """
        Delete all items with the given parent id.

        Return number of items deleted.
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

    def all_ids(
        self, ordered: bool = False, parameters: dict[str, str] | None = None
    ) -> Iterable[str]:
        ids: Iterable[str] = self._items.keys()
        if parameters:
            raise ValueError("parameters not supported for TempBeaker")
        if ordered:
            ids = sorted(ids)
        return ids

    def all_ids_and_parents(self) -> Iterable[tuple[str, str]]:
        return self._parent_ids.items()

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

    def get_item(self, id: str) -> BaseModel:
        try:
            return self._items[id]
        except KeyError:
            raise ItemNotFound(f"{id} not found in {self.name}")

    def delete(
        self, *, parent: list[str] | None = None, ids: list[str] | None = None
    ) -> list[str]:
        deleted = []
        everything = parent is None and ids is None
        for id, parent_id in self._parent_ids.items():
            if (parent and parent_id in parent) or (ids and id in ids) or everything:
                deleted.append(id)
        for id in deleted:
            del self._items[id]
            del self._parent_ids[id]
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
        self._table.create_index(["parent"], if_not_exists=True)
        log.debug("beaker initialized", count=self._table.count, name=self.name)
        # TODO: allow pydantic-to-model here

    def __len__(self) -> int:
        return self._table.count

    def parent_id_set(self) -> set[str]:
        return {row["parent"] for row in self._table.rows_where(select="parent")}

    def all_ids(
        self, ordered: bool = False, where: dict[str, str] | None = None
    ) -> Iterable[str]:
        if where:
            where_str = " and ".join(
                [f"json_extract(data, '$.{k}') = ?" for k in where.keys()]
            )
            where_vals = list(where.values())
            rows = self._table.rows_where(where_str, where_vals, select="uuid")
        else:
            rows = self._table.rows_where(select="uuid")
        ids = [row["uuid"] for row in rows]
        if ordered:
            ids = sorted(ids)
        return ids

    def all_ids_and_parents(self) -> Iterable[tuple[str, str]]:
        return (
            (row["uuid"], row["parent"])
            for row in self._table.rows_where(select="uuid, parent")
        )

    def items(self) -> Iterable[tuple[str, BaseModel]]:
        for item in self._table.rows_where(select="uuid, data"):
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

    def delete(
        self, *, parent: list[str] | None = None, ids: list[str] | None = None
    ) -> list[str]:
        if parent is not None:
            query_string = "parent in ({})".format(",".join("?" * len(parent)))
            query_values = parent
        elif ids is not None:
            query_string = "uuid in ({})".format(",".join("?" * len(ids)))
            query_values = ids
        else:
            query_string = "1=1"
            query_values = []

        ids = [
            row["uuid"]
            for row in self._table.rows_where(query_string, query_values, select="uuid")
        ]
        log.info(
            "beaker delete where",
            beaker=self.name,
            parent=parent,
            deleted=len(ids),
            query_string=query_string,
            query_values=query_values,
        )

        self._table.delete_where(query_string, query_values)

        return ids


class DirectoryBeaker(Beaker):
    def __init__(self, name: str, model: PydanticModel, pipeline: "Pipeline"):
        super().__init__(name, model, pipeline)
        # create table if it doesn't exist
        self._dir = pathlib.Path("_files") / self.name
        self._dir.mkdir(parents=True, exist_ok=True)
        """
        This creates a directory structure like this:

        _files/
            beaker_name/
                parent_id/
                    item_id.ext
        """
        self._count = 0

    def __len__(self) -> int:
        return self._count

    def parent_id_set(self) -> set[str]:
        return set([d.name for d in self._dir.iterdir()])

    def all_ids(
        self, ordered: bool = False, where: dict[str, str] | None = None
    ) -> Iterable[str]:
        if ordered or where:
            raise ValueError("ordered and where not supported for DirectoryBeaker")
        return (item.name for item in self._dir.glob("*/*"))

    def all_ids_and_parents(self) -> Iterable[tuple[str, str]]:
        return ((item.name, item.parent.name) for item in self._dir.glob("*/*"))

    def add_item(
        self, item: BaseModel, *, parent: str | None, id_: str | None = None
    ) -> None:
        if not hasattr(item, "write_to_path"):
            raise TypeError(
                f"beaker {self.name} received {item!r} ({type(item)}), "
                f"expecting an instance of {self.model} (which must have a write_to_path method)"
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
        path = self._dir / parent / id_
        item.write_to_path(path)
        self._count += 1

    def get_item(self, id: str) -> BaseModel:
        raise NotImplementedError("DirectoryBeaker.get_item() not implemented")

    def items(self) -> Iterable[tuple[str, BaseModel]]:
        raise NotImplementedError("DirectoryBeaker.items() not implemented")

    def delete(
        self, *, parent: list[str] | None = None, ids: list[str] | None = None
    ) -> list[str]:
        raise NotImplementedError("DirectoryBeaker.delete() not implemented")
