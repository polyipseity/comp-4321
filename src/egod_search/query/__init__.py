# -*- coding: UTF-8 -*-
from dataclasses import dataclass
from enum import IntEnum, auto
from typing import Sequence


@dataclass(frozen=True, kw_only=True, slots=True)
class ParsedQuery:
    """
    A parsed query.
    """

    terms: Sequence[str]
    """
    A sequence of terms, in order of appearance. Repeated terms are allowed.

    Terms represents text that we want the result to have.
    """
    phrases: Sequence[str]
    """
    A sequence of phrases, in order of appearance. Repeated phrases are allowed.

    Phrases represent text that we require the result to have.
    """


def lex_query(query: str) -> Sequence[str]:
    """
    Decompose a query into its components.
    """
    tokens = list[str]()

    class State(IntEnum):
        TERM = auto()
        PHRASE = auto()

    state = State.TERM
    token = ""

    for char in query:
        match state:
            case State.TERM:
                if token in ' "':
                    if token:
                        tokens.append(token)
                        token = ""
                    if char == '"':
                        state = State.PHRASE
                    continue
                token += char
            case State.PHRASE:
                if char == '"':
                    tokens.append(f'"{token}')
                    token = ""
                    state = State.TERM
                    continue
                token += char
            case _:  # type: ignore
                raise ValueError()

    if token:
        tokens.append(token)

    return tokens


def parse_query(tokens: Sequence[str]) -> ParsedQuery:
    """
    Parse a decomposed query into a data structure.
    """
    ret = ParsedQuery(terms=(terms := list[str]()), phrases=(phrases := list[str]()))
    for token in tokens:
        if token.startswith('"'):
            phrases.append(token[len('"') :])
            continue
        terms.append(token)
    return ret
