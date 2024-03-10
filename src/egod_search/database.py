# -*- coding: UTF-8 -*-
from asyncio import Lock
from json import JSONDecodeError, dumps, loads
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

        __slots__ = ()

    def __init__(self, io: AsyncFile[str]) -> None:
        """
        Create a database with `io` as the underlying storage.
        """
        self._lock = Lock()
        self._io = io

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
                f"Database is not deserializable: {text}"
            ) from exc
        return data

    async def write(self, obj: object) -> None:
        """
        Save the object to the database.

        Raises `InvalidFormat` if the object is not serializable.
        """
        try:
            text = dumps(obj, indent=2)  # evnchn: make the database readable
        except (TypeError, ValueError) as exc:
            raise Database.InvalidFormat(f"Object is not serializable: {obj}") from exc

        async with self._lock:
            await self._io.seek(0)
            await self._io.write(text)
            await self._io.truncate()
