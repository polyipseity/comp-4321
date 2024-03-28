# -*- coding: UTF-8 -*-
from aiosqlite import connect
from datetime import datetime, timezone
from importlib.resources import files
from unittest import IsolatedAsyncioTestCase, TestCase, main

from . import PACKAGE_NAME
from ._util import a_fetch_one, a_fetch_value, parse_http_datetime


class HTTPTestCase(TestCase):
    __slots__ = ()

    def test_parse_http_datetime(self) -> None:
        for input, output in {
            "Thu, 01 Jan 1970 00:00:00 GMT": datetime(
                1970, 1, 1, 0, 0, tzinfo=timezone.utc
            ),
            "Sun, 09 May 2004 04:53:47 GMT": datetime(
                2004, 5, 9, 4, 53, 47, tzinfo=timezone.utc
            ),
            "Fri, 17 Feb 2034 08:30:38 GMT": datetime(
                2034, 2, 17, 8, 30, 38, tzinfo=timezone.utc
            ),
            "Sat, 15 May 1993 04:22:35 GMT": datetime(
                1993, 5, 15, 4, 22, 35, tzinfo=timezone.utc
            ),
            "Thu, 22 Sep 1994 22:35:48 GMT": datetime(
                1994, 9, 22, 22, 35, 48, tzinfo=timezone.utc
            ),
            "Fri, 17 Jun 2022 01:20:21 GMT": datetime(
                2022, 6, 17, 1, 20, 21, tzinfo=timezone.utc
            ),
            "Fri, 11 Aug 2017 12:45:08 GMT": datetime(
                2017, 8, 11, 12, 45, 8, tzinfo=timezone.utc
            ),
            "Tue, 08 Apr 1986 07:56:55 GMT": datetime(
                1986, 4, 8, 7, 56, 55, tzinfo=timezone.utc
            ),
            "Thu, 19 Jun 2014 11:47:57 GMT": datetime(
                2014, 6, 19, 11, 47, 57, tzinfo=timezone.utc
            ),
            "Thu, 06 Jan 2033 01:39:09 GMT": datetime(
                2033, 1, 6, 1, 39, 9, tzinfo=timezone.utc
            ),
            "Fri, 21 Feb 1986 14:43:49 GMT": datetime(
                1986, 2, 21, 14, 43, 49, tzinfo=timezone.utc
            ),
            "Sat, 20 Apr 2019 12:21:03 GMT": datetime(
                2019, 4, 20, 12, 21, 3, tzinfo=timezone.utc
            ),
            "Tue, 02 Dec 2025 13:41:46 GMT": datetime(
                2025, 12, 2, 13, 41, 46, tzinfo=timezone.utc
            ),
            "Thu, 20 Sep 2007 23:40:29 GMT": datetime(
                2007, 9, 20, 23, 40, 29, tzinfo=timezone.utc
            ),
            "Fri, 20 Jul 1979 17:20:06 GMT": datetime(
                1979, 7, 20, 17, 20, 6, tzinfo=timezone.utc
            ),
            "Thu, 02 May 1991 09:21:17 GMT": datetime(
                1991, 5, 2, 9, 21, 17, tzinfo=timezone.utc
            ),
            "Fri, 08 Jun 2007 12:26:39 GMT": datetime(
                2007, 6, 8, 12, 26, 39, tzinfo=timezone.utc
            ),
            "Sat, 23 Mar 1974 08:40:24 GMT": datetime(
                1974, 3, 23, 8, 40, 24, tzinfo=timezone.utc
            ),
            "Fri, 19 Apr 2013 02:12:48 GMT": datetime(
                2013, 4, 19, 2, 12, 48, tzinfo=timezone.utc
            ),
            "Wed, 15 Mar 1989 17:22:15 GMT": datetime(
                1989, 3, 15, 17, 22, 15, tzinfo=timezone.utc
            ),
            "Sat, 06 Jul 1974 13:02:27 GMT": datetime(
                1974, 7, 6, 13, 2, 27, tzinfo=timezone.utc
            ),
            "Sat, 04 Oct 2014 10:36:59 GMT": datetime(
                2014, 10, 4, 10, 36, 59, tzinfo=timezone.utc
            ),
            "Wed, 02 Sep 1992 15:41:56 GMT": datetime(
                1992, 9, 2, 15, 41, 56, tzinfo=timezone.utc
            ),
            "Wed, 04 Oct 1972 22:45:27 GMT": datetime(
                1972, 10, 4, 22, 45, 27, tzinfo=timezone.utc
            ),
            "Tue, 14 Mar 1995 23:32:39 GMT": datetime(
                1995, 3, 14, 23, 32, 39, tzinfo=timezone.utc
            ),
            "Sat, 29 Jul 1978 08:02:02 GMT": datetime(
                1978, 7, 29, 8, 2, 2, tzinfo=timezone.utc
            ),
            "Mon, 21 Dec 1981 04:06:08 GMT": datetime(
                1981, 12, 21, 4, 6, 8, tzinfo=timezone.utc
            ),
            "Wed, 20 Sep 1972 06:38:59 GMT": datetime(
                1972, 9, 20, 6, 38, 59, tzinfo=timezone.utc
            ),
            "Sat, 25 Jul 2009 00:48:48 GMT": datetime(
                2009, 7, 25, 0, 48, 48, tzinfo=timezone.utc
            ),
            "Sat, 29 Jun 2002 13:40:51 GMT": datetime(
                2002, 6, 29, 13, 40, 51, tzinfo=timezone.utc
            ),
            "Thu, 01 Oct 1970 01:57:05 GMT": datetime(
                1970, 10, 1, 1, 57, 5, tzinfo=timezone.utc
            ),
            "Fri, 23 Nov 2035 20:39:37 GMT": datetime(
                2035, 11, 23, 20, 39, 37, tzinfo=timezone.utc
            ),
            "Tue, 11 May 2021 09:38:48 GMT": datetime(
                2021, 5, 11, 9, 38, 48, tzinfo=timezone.utc
            ),
            "Mon, 05 May 2014 08:12:56 GMT": datetime(
                2014, 5, 5, 8, 12, 56, tzinfo=timezone.utc
            ),
            "Mon, 15 Jun 2037 05:53:02 GMT": datetime(
                2037, 6, 15, 5, 53, 2, tzinfo=timezone.utc
            ),
            "Sun, 20 Aug 2034 07:39:13 GMT": datetime(
                2034, 8, 20, 7, 39, 13, tzinfo=timezone.utc
            ),
            "Mon, 29 Apr 2024 14:38:11 GMT": datetime(
                2024, 4, 29, 14, 38, 11, tzinfo=timezone.utc
            ),
            "Mon, 11 Aug 2036 08:35:32 GMT": datetime(
                2036, 8, 11, 8, 35, 32, tzinfo=timezone.utc
            ),
            "Wed, 25 Oct 1972 20:50:43 GMT": datetime(
                1972, 10, 25, 20, 50, 43, tzinfo=timezone.utc
            ),
            "Mon, 18 Jul 1994 07:43:04 GMT": datetime(
                1994, 7, 18, 7, 43, 4, tzinfo=timezone.utc
            ),
            "Sat, 09 Jul 1988 22:52:34 GMT": datetime(
                1988, 7, 9, 22, 52, 34, tzinfo=timezone.utc
            ),
            "Thu, 24 Jul 1980 23:51:38 GMT": datetime(
                1980, 7, 24, 23, 51, 38, tzinfo=timezone.utc
            ),
            "Fri, 13 Feb 2009 07:06:05 GMT": datetime(
                2009, 2, 13, 7, 6, 5, tzinfo=timezone.utc
            ),
            "Mon, 21 Sep 2009 15:49:46 GMT": datetime(
                2009, 9, 21, 15, 49, 46, tzinfo=timezone.utc
            ),
            "Fri, 30 Mar 2001 08:03:17 GMT": datetime(
                2001, 3, 30, 8, 3, 17, tzinfo=timezone.utc
            ),
            "Fri, 27 May 2011 12:16:19 GMT": datetime(
                2011, 5, 27, 12, 16, 19, tzinfo=timezone.utc
            ),
            "Thu, 11 Sep 1997 15:51:27 GMT": datetime(
                1997, 9, 11, 15, 51, 27, tzinfo=timezone.utc
            ),
            "Fri, 31 Jul 1970 13:32:17 GMT": datetime(
                1970, 7, 31, 13, 32, 17, tzinfo=timezone.utc
            ),
            "Sat, 31 Aug 1991 11:33:51 GMT": datetime(
                1991, 8, 31, 11, 33, 51, tzinfo=timezone.utc
            ),
            "Thu, 23 Jan 2014 14:45:25 GMT": datetime(
                2014, 1, 23, 14, 45, 25, tzinfo=timezone.utc
            ),
            "Mon, 06 Apr 1998 20:48:53 GMT": datetime(
                1998, 4, 6, 20, 48, 53, tzinfo=timezone.utc
            ),
            "Mon, 16 Dec 1985 20:23:25 GMT": datetime(
                1985, 12, 16, 20, 23, 25, tzinfo=timezone.utc
            ),
            "Sun, 03 Dec 2034 11:00:48 GMT": datetime(
                2034, 12, 3, 11, 0, 48, tzinfo=timezone.utc
            ),
            "Fri, 17 Dec 1982 00:07:40 GMT": datetime(
                1982, 12, 17, 0, 7, 40, tzinfo=timezone.utc
            ),
            "Thu, 05 May 1983 19:45:16 GMT": datetime(
                1983, 5, 5, 19, 45, 16, tzinfo=timezone.utc
            ),
            "Sun, 20 Jan 2002 01:25:14 GMT": datetime(
                2002, 1, 20, 1, 25, 14, tzinfo=timezone.utc
            ),
            "Sat, 21 Apr 1984 17:22:10 GMT": datetime(
                1984, 4, 21, 17, 22, 10, tzinfo=timezone.utc
            ),
            "Tue, 26 May 1987 01:44:28 GMT": datetime(
                1987, 5, 26, 1, 44, 28, tzinfo=timezone.utc
            ),
            "Wed, 03 Jan 2007 22:22:32 GMT": datetime(
                2007, 1, 3, 22, 22, 32, tzinfo=timezone.utc
            ),
            "Tue, 12 Jul 2005 13:06:19 GMT": datetime(
                2005, 7, 12, 13, 6, 19, tzinfo=timezone.utc
            ),
            "Fri, 27 Nov 1970 20:17:27 GMT": datetime(
                1970, 11, 27, 20, 17, 27, tzinfo=timezone.utc
            ),
            "Sun, 21 Apr 1974 16:15:54 GMT": datetime(
                1974, 4, 21, 16, 15, 54, tzinfo=timezone.utc
            ),
            "Wed, 30 Dec 1987 20:39:08 GMT": datetime(
                1987, 12, 30, 20, 39, 8, tzinfo=timezone.utc
            ),
            "Thu, 28 Sep 2023 01:00:52 GMT": datetime(
                2023, 9, 28, 1, 0, 52, tzinfo=timezone.utc
            ),
        }.items():
            self.assertEqual(output, parse_http_datetime(input))
        for input in (
            "Sun, 31 Sep 2023 01:00:52 GMT",
            "23:01, 17 Jul 2013",
            "2001-01-04 01:56:50",
            "August 12, 2021, at 00:10 PM",
            "Wednesday, 9 December, 2020, 01:47 GMT 02:47 UK",
            "6/Jul/22 10:01:01",
            "2028-01-28 13:59:01",
            "Thursday, July 5, 2018 10:07:12 pm",
            "Mon, 09 Oct 2045",
            "23 Feb 01, 12:37",
            "12 Aug 2047 - 21:05",
            "2000-10-15 10:50",
            "2029/04/20 23:57",
            "00:00, 10 August 2012",
        ):
            with self.assertRaises(ValueError, msg=input):
                parse_http_datetime(input)


class SQLiteTestCase(IsolatedAsyncioTestCase):
    __slots__ = ("_conn",)

    # @override
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self._conn = await connect("")
        await self._conn.executescript(
            (files(PACKAGE_NAME) / "res/Chinook_Sqlite.sql").read_text()
        )

    async def test_a_fetch_one(self) -> None:
        self.assertSequenceEqual(
            (3503, "Koyaanisqatsi", "Philip Glass"),
            await a_fetch_one(
                self._conn,
                """
SELECT TrackId, Name, Composer FROM main.Track
ORDER BY TrackId DESC
LIMIT 50""",
            ),
        )
        self.assertEqual(
            None,
            await a_fetch_one(
                self._conn,
                """
SELECT TrackId, Name, Composer FROM main.Track
ORDER BY TrackId DESC
LIMIT 0""",
            ),
        )

    async def test_a_fetch_value(self) -> None:
        self.assertEqual(
            3503,
            await a_fetch_value(
                self._conn,
                """
SELECT TrackId, Name, Composer FROM main.Track
ORDER BY TrackId DESC
LIMIT 50""",
            ),
        )
        self.assertEqual(
            None,
            await a_fetch_value(
                self._conn,
                """
SELECT TrackId, Name, Composer FROM main.Track
ORDER BY TrackId DESC
LIMIT 0""",
            ),
        )
        self.assertEqual(
            ...,
            await a_fetch_value(
                self._conn,
                """
SELECT TrackId, Name, Composer FROM main.Track
ORDER BY TrackId DESC
LIMIT 0""",
                default=...,
            ),
        )

    # @override
    async def asyncTearDown(self) -> None:
        await self._conn.close()
        return await super().asyncTearDown()


if __name__ == "__main__":
    main()
