from __future__ import annotations


class DataParseError:
    __slots__ = ('attrs',)

    def __init__(self, **attrs: str | int | None) -> None:
        self.attrs = attrs


class NoAvailableServerException(Exception):
    pass


class ParsingError(Exception):
    pass
