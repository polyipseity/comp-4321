# -*- coding: UTF-8 -*-
from asyncio import Lock
from json import JSONDecodeError, dumps, loads
from types import TracebackType
from typing import Any, Callable, Type
from anyio import AsyncFile


class Database:
    """
    Database that stores serializable objects.
    """

    __slots__ = ("_io", "_lock")

    class InvalidFormat(Exception):
        """
        Exception for invalid object format or database format.
        """

        pass

    def __init__(self, io_supplier: Callable[[], AsyncFile[str]]) -> None:
        self._lock = Lock()
        self._io = io_supplier()

    async def __aenter__(self) -> "Database":
        await self._io.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """
        Cleanup the database.
        """
        await self._io.aclose()

    async def clear(self) -> None:
        """
        Clear the database.
        """
        async with self._lock:
            await self._io.seek(0)
            await self._io.truncate()

    async def read(self) -> object:
        """
        Read the object from the database.

        Raises `InvalidFormat` if the database is not unserializable.
        """
        async with self._lock:
            await self._io.seek(0)
            text = await self._io.read()

        try:
            data = loads(text)
        except JSONDecodeError as exc:
            raise Database.InvalidFormat(
                f"Database is not unserializable: {text}"
            ) from exc
        return data

    async def write(self, obj: Any) -> None:
        """
        Save the object to the database.

        Raises `InvalidFormat` if the object is not serializable.
        """
        try:
            text = dumps(obj)
        except (TypeError, ValueError) as exc:
            raise Database.InvalidFormat(f"Object is not serializable: {obj}") from exc

        async with self._lock:
            await self._io.seek(0)
            await self._io.write(text)
            await self._io.truncate()
