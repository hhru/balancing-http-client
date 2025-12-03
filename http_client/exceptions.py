from __future__ import annotations


class DataParseError:
    __slots__ = ('attrs',)

    def __init__(self, **attrs: str | int | None) -> None:
        self.attrs = attrs


class NoAvailableServerError(Exception):
    pass


class ParsingError(Exception):
    pass
