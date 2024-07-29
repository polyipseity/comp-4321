from dataclasses import dataclass
from enum import IntEnum, auto
from typing import Literal, Sequence


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


@dataclass(frozen=True, kw_only=True, slots=True)
class QueryToken:
    """
    A query component.
    """

    type: Literal["term", "phrase"]
    """
    Type of query component.
    """
    value: str
    """
    Contents of query component.
    """


def lex_query(query: str) -> Sequence[QueryToken]:
    """
    Decompose a query into its components.
    """
    tokens = list[QueryToken]()

    class State(IntEnum):
        TERM = auto()
        PHRASE = auto()

    state = State.TERM
    token = ""

    for char in query:
        match state:
            case State.TERM:
                # find raw stem words in the input field splitted by SPACE CHARACTER
                if char in ' "':
                    if token:
                        tokens.append(QueryToken(type="term", value=token))
                        token = ""
                    if char == '"':
                        state = State.PHRASE
                    continue
                token += char
            case State.PHRASE:
                # Look for phrases in the input field which are surrounded by double quotes, which needs special attention. It is stored in phrase variable
                if char == '"':
                    tokens.append(QueryToken(type="phrase", value=token))
                    tokens.extend(
                        QueryToken(type="term", value=tk) for tk in token.split(" ")
                    )
                    token = ""
                    state = State.TERM
                    continue
                token += char
            case _:  # type: ignore
                raise ValueError()

    if token:
        tokens.append(QueryToken(type="term", value=token))

    return tokens


def parse_query(tokens: Sequence[QueryToken]) -> ParsedQuery:
    """
    Parse a decomposed query into a parsed query.
    """
    ret = ParsedQuery(terms=(terms := list[str]()), phrases=(phrases := list[str]()))
    for token in tokens:
        match token.type:
            case "term":
                terms.append(token.value)
            case "phrase":
                phrases.append(token.value)
            case _:  # type: ignore
                raise ValueError(token.type)
    return ret
