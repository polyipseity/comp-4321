from asyncio import TaskGroup, gather, to_thread
from importlib.resources import files
from os import cpu_count
from unicodedata import normalize
from unittest import TestCase, main

from .. import PACKAGE_NAME
from .._util import AsyncTestCase, DEFAULT_MULTIPROCESSING_CONTEXT
from .transform import default_transform, normalize_text_for_search, porter, split_words


class TextTestCase(TestCase):
    __slots__ = ()

    def test_default_transform(self) -> None:
        for input, output in {
            "": (),
            "Hi": ((0, "hi"),),
            "Hello world!": ((0, "hello"), (6, "world")),
            "Hello  world!": ((0, "hello"), (7, "world")),
            "Hello                                                  world!": (
                (0, "hello"),
                (55, "world"),
            ),
            "abcdefghijklmnopqrstuvwxyz": ((0, "abcdefghijklmnopqrstuvwxyz"),),
            "日月金木水火土竹戈十大中一弓人心手口尸廿山女田難卜重": (
                (0, "日月金木水火土竹戈十大中一弓人心手口尸廿山女田難卜重"),
            ),
            "Sneed's Feed and Seed": (
                (0, "sneed"),
                (8, "feed"),
                (17, "seed"),
            ),
            "Formerly Chuck's": ((0, "formerli"), (9, "chuck")),
            "`~!@#$%^&*()-+[{]}\\|;:'\",<.>/?": (),
            "`~1!2@3#4$5%6^7&8*9(0)-+[{]}\\|;:'\",<.>/?": (
                (0, "1"),
                (4, "2"),
                (6, "3"),
                (8, "4"),
                (10, "5"),
                (12, "67"),
                (16, "89"),
                (20, "0"),
            ),
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit, "
            "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. "
            "Ut enim ad minim veniam, "
            "quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. "
            "Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. "
            "Excepteur sint occaecat cupidatat non proident, "
            "sunt in culpa qui officia deserunt mollit anim id est laborum.": (
                (0, "lorem"),
                (6, "ipsum"),
                (12, "dolor"),
                (18, "sit"),
                (22, "amet"),
                (28, "consectetur"),
                (40, "adipisc"),
                (51, "elit"),
                (57, "sed"),
                (64, "eiusmod"),
                (72, "tempor"),
                (79, "incididunt"),
                (90, "ut"),
                (93, "labor"),
                (100, "et"),
                (103, "dolor"),
                (110, "magna"),
                (116, "aliqua"),
                (124, "ut"),
                (127, "enim"),
                (132, "ad"),
                (135, "minim"),
                (141, "veniam"),
                (149, "qui"),
                (154, "nostrud"),
                (162, "exercit"),
                (175, "ullamco"),
                (183, "labori"),
                (191, "nisi"),
                (196, "ut"),
                (199, "aliquip"),
                (207, "ex"),
                (210, "ea"),
                (213, "commodo"),
                (221, "consequat"),
                (232, "dui"),
                (237, "aut"),
                (242, "irur"),
                (248, "dolor"),
                (257, "reprehenderit"),
                (274, "volupt"),
                (284, "velit"),
                (290, "ess"),
                (295, "cillum"),
                (302, "dolor"),
                (309, "eu"),
                (312, "fugiat"),
                (319, "nulla"),
                (325, "pariatur"),
                (335, "excepteur"),
                (345, "sint"),
                (350, "occaecat"),
                (359, "cupidatat"),
                (373, "proident"),
                (383, "sunt"),
                (391, "culpa"),
                (397, "qui"),
                (401, "officia"),
                (409, "deserunt"),
                (418, "mollit"),
                (425, "anim"),
                (430, "id"),
                (433, "est"),
                (437, "laborum"),
            ),
        }.items():
            self.assertTupleEqual(output, tuple(default_transform(input)))

    def test_split_words(self) -> None:
        for input, output in {
            "": (),
            "Hi": ((0, "Hi"),),
            "Hello world!": ((0, "Hello"), (6, "world"), (11, "!")),
            "Hello  world!": ((0, "Hello"), (7, "world"), (12, "!")),
            "Hello                                                  world!": (
                (0, "Hello"),
                (55, "world"),
                (60, "!"),
            ),
            "abcdefghijklmnopqrstuvwxyz": ((0, "abcdefghijklmnopqrstuvwxyz"),),
            "日月金木水火土竹戈十大中一弓人心手口尸廿山女田難卜重": (
                (0, "日月金木水火土竹戈十大中一弓人心手口尸廿山女田難卜重"),
            ),
            "Sneed's Feed and Seed": (
                (0, "Sneed"),
                (5, "'s"),
                (8, "Feed"),
                (13, "and"),
                (17, "Seed"),
            ),
            "Formerly Chuck's": ((0, "Formerly"), (9, "Chuck"), (14, "'s")),
            "`~!@#$%^&*()-+[{]}\\|;:'\",<.>/?": (
                (0, "`~"),
                (2, "!"),
                (3, "@"),
                (4, "#"),
                (5, "$"),
                (6, "%"),
                (7, "^"),
                (8, "&"),
                (9, "*"),
                (10, "("),
                (11, ")"),
                (12, "-+"),
                (14, "["),
                (15, "{"),
                (16, "]"),
                (17, "}"),
                (18, "\\|"),
                (20, ";"),
                (21, ":"),
                (22, "'"),
                (23, '"'),
                (24, ","),
                (25, "<"),
                (26, "."),
                (27, ">"),
                (28, "/"),
                (29, "?"),
            ),
            "`~1!2@3#4$5%6^7&8*9(0)-+[{]}\\|;:'\",<.>/?": (
                (0, "`~1"),
                (3, "!"),
                (4, "2"),
                (5, "@"),
                (6, "3"),
                (7, "#"),
                (8, "4"),
                (9, "$"),
                (10, "5"),
                (11, "%"),
                (12, "6^7"),
                (15, "&"),
                (16, "8*9"),
                (19, "("),
                (20, "0"),
                (21, ")"),
                (22, "-+"),
                (24, "["),
                (25, "{"),
                (26, "]"),
                (27, "}"),
                (28, "\\|"),
                (30, ";"),
                (31, ":"),
                (32, "'"),
                (33, '"'),
                (34, ","),
                (35, "<"),
                (36, "."),
                (37, ">"),
                (38, "/"),
                (39, "?"),
            ),
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit, "
            "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. "
            "Ut enim ad minim veniam, "
            "quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. "
            "Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. "
            "Excepteur sint occaecat cupidatat non proident, "
            "sunt in culpa qui officia deserunt mollit anim id est laborum.": (
                (0, "Lorem"),
                (6, "ipsum"),
                (12, "dolor"),
                (18, "sit"),
                (22, "amet"),
                (26, ","),
                (28, "consectetur"),
                (40, "adipiscing"),
                (51, "elit"),
                (55, ","),
                (57, "sed"),
                (61, "do"),
                (64, "eiusmod"),
                (72, "tempor"),
                (79, "incididunt"),
                (90, "ut"),
                (93, "labore"),
                (100, "et"),
                (103, "dolore"),
                (110, "magna"),
                (116, "aliqua."),
                (124, "Ut"),
                (127, "enim"),
                (132, "ad"),
                (135, "minim"),
                (141, "veniam"),
                (147, ","),
                (149, "quis"),
                (154, "nostrud"),
                (162, "exercitation"),
                (175, "ullamco"),
                (183, "laboris"),
                (191, "nisi"),
                (196, "ut"),
                (199, "aliquip"),
                (207, "ex"),
                (210, "ea"),
                (213, "commodo"),
                (221, "consequat."),
                (232, "Duis"),
                (237, "aute"),
                (242, "irure"),
                (248, "dolor"),
                (254, "in"),
                (257, "reprehenderit"),
                (271, "in"),
                (274, "voluptate"),
                (284, "velit"),
                (290, "esse"),
                (295, "cillum"),
                (302, "dolore"),
                (309, "eu"),
                (312, "fugiat"),
                (319, "nulla"),
                (325, "pariatur."),
                (335, "Excepteur"),
                (345, "sint"),
                (350, "occaecat"),
                (359, "cupidatat"),
                (369, "non"),
                (373, "proident"),
                (381, ","),
                (383, "sunt"),
                (388, "in"),
                (391, "culpa"),
                (397, "qui"),
                (401, "officia"),
                (409, "deserunt"),
                (418, "mollit"),
                (425, "anim"),
                (430, "id"),
                (433, "est"),
                (437, "laborum"),
                (444, "."),
            ),
        }.items():
            self.assertTupleEqual(output, tuple(split_words(input)))


def _word_test_porter(input: tuple[int, str]):
    return input[0], porter(input[1])


class WordTestCase(AsyncTestCase):
    __slots__ = ()

    _MP_POOL_CONCURRENCY = cpu_count() or 2
    _MP_POOL_CHUNK_SIZE = _MP_POOL_CONCURRENCY * 4

    def test_normalize_text_for_search(self) -> None:
        for input, output in {
            "": "",
            "abcdefghijklmnopqrstuvwxyz": "abcdefghijklmnopqrstuvwxyz",
            "日月金木水火土竹戈十大中一弓人心手口尸廿山女田難卜重": "日月金木水火土竹戈十大中一弓人心手口尸廿山女田難卜重",
            "Sneed's Feed and Seed": "sneedsfeedandseed",
            "Formerly Chuck's": "formerlychucks",
            "`~!@#$%^&*()-+[{]}\\|;:'\",<.>/?": "",
            "`~1!2@3#4$5%6^7&8*9(0)-+[{]}\\|;:'\",<.>/?": "1234567890",
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit, "
            "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. "
            "Ut enim ad minim veniam, "
            "quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. "
            "Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. "
            "Excepteur sint occaecat cupidatat non proident, "
            "sunt in culpa qui officia deserunt mollit anim id est laborum.": (
                "loremipsumdolorsitametconsecteturadipiscingelit"
                "seddoeiusmodtemporincididuntutlaboreetdoloremagnaaliqua"
                "utenimadminimveniam"
                "quisnostrudexercitationullamcolaborisnisiutaliquipexeacommodoconsequat"
                "duisauteiruredolorinreprehenderitinvoluptatevelitessecillumdoloreeufugiatnullapariatur"
                "excepteursintoccaecatcupidatatnonproident"
                "suntinculpaquiofficiadeseruntmollitanimidestlaborum"
            ),
            "".join(map(chr, range(256))): (
                "0123456789abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
                "a23μ1o141234aaaaaaæceeeeiiiiðnoooooøuuuuyþßaaaaaaæceeeeiiiiðnoooooøuuuuyþy"
            ),
            normalize("NFC", "Beyoncé"): "beyonce",
            normalize("NFD", "Beyoncé"): "beyonce",
            normalize("NFKC", "Beyoncé"): "beyonce",
            normalize("NFKD", "Beyoncé"): "beyonce",
        }.items():
            self.assertEqual(output, normalize_text_for_search(input))

    async def test_porter_mp(self) -> None:
        input, output = await gather(
            to_thread((files(PACKAGE_NAME) / "res/words.txt").read_text),
            to_thread(
                (files(PACKAGE_NAME) / "res/tests/porter_mp/expected.txt").read_text
            ),
        )
        inputs = input.splitlines()
        actual_outputs = [""] * len(inputs)

        with DEFAULT_MULTIPROCESSING_CONTEXT.Pool(self._MP_POOL_CONCURRENCY) as pool:

            def process():
                for idx, actual_output in pool.imap_unordered(
                    _word_test_porter, enumerate(inputs), self._MP_POOL_CHUNK_SIZE
                ):
                    actual_outputs[idx] = actual_output

            async with TaskGroup() as tg:
                process_task = tg.create_task(to_thread(process))
                outputs = output.splitlines()
                await process_task
        self.assertListEqual(outputs, actual_outputs)


if __name__ == "__main__":
    main()
