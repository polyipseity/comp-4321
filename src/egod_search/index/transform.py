# -*- coding: UTF-8 -*-
from functools import cache, wraps
from importlib.resources import files
from itertools import islice, pairwise, tee
from nltk.tokenize import TreebankWordTokenizer  # type: ignore
from typing import Iterator
from unicodedata import normalize

from .. import PACKAGE_NAME

_WORD_TOKENIZER = TreebankWordTokenizer()

STOP_WORDS = frozenset(
    word.casefold()
    for word in (files(PACKAGE_NAME) / "res/stop words.txt").read_text().splitlines()
)
"""
Set of stop words.
"""


def default_transform(text: str) -> Iterator[tuple[int, str]]:
    """
    Default text transformation pipeline.
    """
    words = split_words(text)
    words = ((pos, normalize_text_for_search(word)) for pos, word in words)
    words = ((pos, word) for pos, word in words if word not in STOP_WORDS)
    words = ((pos, porter(word)) for pos, word in words)
    return ((pos, word) for pos, word in words if word)


def default_transform_word(word: str) -> str:
    """
    Default text transformation pipeline for a word.
    """
    word = normalize_text_for_search(word)
    if word in STOP_WORDS:
        return ""
    return porter(word)


def normalize_text_for_search(text: str) -> str:
    """
    Normalize text for searching by doing the following:
    - Normalize the word into Unicode Normalization Compatibility Form D (NFKD).
      This is for removing diacritics in the next step.
      Also, very similar looking characters are converted into the normal characters, such as `𝐀` to `A`.
    - Remove non-alphanumeric characters. This also removes diacritics.
    - Normalize the word into Unicode Normalization Compatibility Form C (NFKC).
      This merges decomposed characters back into their normal form.
    - Convert to lowercase.
    """
    text = normalize("NFKD", text)
    text = "".join(filter(str.isalnum, text))
    text = normalize("NFKC", text)
    text = text.lower()
    return text


class _Porter:
    _LSZ = frozenset("lsz")
    _NOT_SEMIVOWELS = frozenset({"ay", "ey", "iy", "oy", "uy"})
    _PREFIXES = (
        "kilo",
        "micro",
        "milli",
        "intra",
        "ultra",
        "mega",
        "nano",
        "pico",
        "pseudo",
    )
    _STEP2_REPLACEMENTS = {
        "ational": "ate",
        "tional": "tion",
        "enci": "ence",
        "anci": "ance",
        "izer": "ize",
        "iser": "ize",
        "abli": "able",
        "alli": "al",
        "entli": "ent",
        "eli": "e",
        "ousli": "ous",
        "ization": "ize",
        "isation": "ize",
        "ation": "ate",
        "ator": "ate",
        "alism": "al",
        "iveness": "ive",
        "fulness": "ful",
        "ousness": "ous",
        "aliti": "al",
        "iviti": "ive",
        "biliti": "ble",
    }
    _STEP3_REPLACEMENTS = {
        "icate": "ic",
        "ative": "",
        "alize": "al",
        "alise": "al",
        "iciti": "ic",
        "ical": "ic",
        "ful": "",
        "ness": "",
    }
    _STEP4_REPLACEMENTS = (
        "al",
        "ance",
        "ence",
        "er",
        "ic",
        "able",
        "ible",
        "ant",
        "ement",
        "ment",
        "ent",
        "sion",
        "tion",
        "ou",
        "ism",
        "ate",
        "iti",
        "ous",
        "ive",
        "ize",
        "ise",
    )
    _VOWELS = frozenset({"a", "e", "i", "o", "u", "y"})
    _WXY = frozenset("wxy")
    __slots__ = ()

    def __call__(self, word: str) -> str:
        """
        The Porter stemming algorithm.
        """
        word = normalize_text_for_search(word)
        if len(word) <= 2:
            return word
        return self.strip_suffix(self.strip_prefix(word))

    def cvc(self, word: str) -> bool:
        """
        Return whether the word is in CVC format.
        """
        return (
            len(word) >= 3
            and word[-1] not in self._WXY
            and not self.is_vowel_segment(word[-2:])
            and self.is_vowel_segment(word[-3:-1])
            and not self.is_vowel_segment(
                f"?{word[-3]}" if len(word) == 3 else word[-4:-2]
            )
        )

    def contain_vowels(self, word: str) -> bool:
        """
        Return whether the word contains any vowels.
        """
        return any(map(self.is_vowel_segment, map("".join, pairwise(f"a{word}"))))

    def is_vowel_segment(self, segment: str) -> bool:
        """
        Return if two-character `chars` is a vowel.
        """
        assert len(segment) == 2
        return segment[-1] in self._VOWELS and segment not in self._NOT_SEMIVOWELS

    def measure_vowel_segments(self, word: str) -> int:
        """
        Measure the number of vowel segments.
        """
        vowels = map(self.is_vowel_segment, map("".join, pairwise(f"a{word}")))
        vowels, next_vowels = tee(vowels, 2)
        return sum(
            1
            for state in zip(vowels, islice(next_vowels, 1, None))
            if state == (True, False)
        )

    def strip_prefix(self, word: str) -> str:
        """
        Strip prefix from word if any.
        """
        if not (ret := word):
            return ""
        for prefix in self._PREFIXES:
            if (ret := word.removeprefix(prefix)) != word:
                break
        return ret

    def strip_suffix(self, word: str) -> str:
        """
        Strip suffix from word if any.
        """
        for step in (self.step1, self.step2, self.step3, self.step4, self.step5):
            if not word:
                return ""
            word = step(word)
        return word

    def step1(self, word: str) -> str:
        """
        Step 1 of the Porter stemming algorithm.
        """
        if word.endswith("s"):
            if word.endswith(("sses", "ies")) and word not in {"sses", "ies"}:
                word = word[:-2]
            else:
                if len(word) == 1:
                    return ""
                if word[-2] != "s":
                    word = word[:-1]
        if word.endswith("eed") and len(word) > 3:
            if self.measure_vowel_segments(word[:-3]) > 0:
                word = word[:-1]
        elif (
            (word2 := word.removesuffix("ed")) != word
            or (word2 := word.removesuffix("ing")) != word
        ) and self.contain_vowels(word2):
            word = word2
            if len(word) <= 1:
                return word
            if word.endswith(("at", "bl", "iz")) and len(word) > 2:
                word += "e"
            else:
                if word[-1] not in self._LSZ and word[-1] == word[-2]:
                    word = word[:-1]
                elif self.measure_vowel_segments(word) == 1 and self.cvc(word):
                    word += "e"
        if word.endswith("y") and self.contain_vowels(word[:-1]):
            word = f"{word[:-1]}i"
        return word

    def step2(self, word: str) -> str:
        """
        Step 2 of the Porter stemming algorithm.
        """
        for find, replace in self._STEP2_REPLACEMENTS.items():
            if (
                word2 := word.removesuffix(find)
            ) != word and self.measure_vowel_segments(word2) > 0:
                return f"{word2}{replace}"
        return word

    def step3(self, word: str) -> str:
        """
        Step 3 of the Porter stemming algorithm.
        """
        for find, replace in self._STEP3_REPLACEMENTS.items():
            if (
                word2 := word.removesuffix(find)
            ) != word and self.measure_vowel_segments(word2) > 0:
                return f"{word2}{replace}"
        return word

    def step4(self, word: str) -> str:
        """
        Step 4 of the Porter stemming algorithm.
        """
        for find in self._STEP4_REPLACEMENTS:
            if (
                word2 := word.removesuffix(find)
            ) != word and self.measure_vowel_segments(word2) > 1:
                return word2
        return word

    def step5(self, word: str) -> str:
        """
        Step 5 of the Porter stemming algorithm.
        """
        if word[-1] == "e":
            if (measure := self.measure_vowel_segments(word)) > 1:
                word = word[:-1]
            elif measure == 1 and not self.cvc(word2 := word[:-1]):
                word = word2
        if len(word) == 1:
            return word
        if word[-2:] == "ll" and self.measure_vowel_segments(word) > 1:
            word = word[:-1]
        return word


_porter = _Porter()


@wraps(_porter)
@cache
def porter(word: str) -> str:
    return _porter(word)


def split_words(text: str) -> Iterator[tuple[int, str]]:
    """
    Split text into a sequence of positions and words.

    It uses `TreebankWordTokenizer`. See its documentation for details.
    """
    for start, end in _WORD_TOKENIZER.span_tokenize(text):
        yield start, text[start:end]
