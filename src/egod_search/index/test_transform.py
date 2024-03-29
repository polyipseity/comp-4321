# -*- coding: UTF-8 -*-
from unittest import TestCase, main

from .transform import normalize_text_for_search


class WordTestCase(TestCase):
    def test_clean_word(self) -> None:
        for input, output in {
            "": "",
            "abcdefghijklmnopqrstuvwxyz": "abcdefghijklmnopqrstuvwxyz",
            "日月金木水火土竹戈十大中一弓人心手口尸廿山女田難卜重": "日月金木水火土竹戈十大中一弓人心手口尸廿山女田難卜重",
            "Sneed's Feed and Seed": "sneedsfeedandseed",
            "Formerly Chuck's": "formerlychucks",
            "`~!@#$%^&*()-+[{]}\\|;:'\",<.>/?": "",
            "`~1!2@3#4$5%6^7&8*9(0)-+[{]}\\|;:'\",<.>/?": "1234567890",
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.": "loremipsumdolorsitametconsecteturadipiscingelitseddoeiusmodtemporincididuntutlaboreetdoloremagnaaliquautenimadminimveniamquisnostrudexercitationullamcolaborisnisiutaliquipexeacommodoconsequatduisauteiruredolorinreprehenderitinvoluptatevelitessecillumdoloreeufugiatnullapariaturexcepteursintoccaecatcupidatatnonproidentsuntinculpaquiofficiadeseruntmollitanimidestlaborum",
            "".join(
                map(chr, range(256))
            ): "0123456789abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzª²³µ¹º¼½¾àáâãäåæçèéêëìíîïðñòóôõöøùúûüýþßàáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿ",
        }.items():
            self.assertEqual(output, normalize_text_for_search(input))


if __name__ == "__main__":
    main()
