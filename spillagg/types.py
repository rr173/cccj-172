"""Typed value handling for the spillable aggregation engine.

Field types are fixed by the declaration:

* ``text``  -> Python ``str`` (nullable)
* ``bool``  -> Python ``bool`` (nullable).  bool is *not* treated as int.
* ``int``   -> Python ``int`` (nullable, arbitrary precision).

The single ``NULL`` sentinel represents a real null value.  It sorts before
every non-null value and compares equal only to itself.  Note that ``bool``
instances are rejected where an ``int`` is required (``isinstance(True, int)``
is True in Python, hence the explicit guard everywhere).
"""

from __future__ import annotations

from typing import Any, Tuple


class _Null:
    """Singleton representing SQL NULL inside in-memory tuples.

    Total order: NULL < everything else.  Equal/identical to itself, hashable
    so it can be used as a dict key for group coordinates.
    """

    _instance: "_Null | None" = None

    def __new__(cls) -> "_Null":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __lt__(self, other: Any) -> bool:
        return other is not self

    def __le__(self, other: Any) -> bool:
        return True

    def __gt__(self, other: Any) -> bool:
        return False

    def __ge__(self, other: Any) -> bool:
        return other is self

    def __eq__(self, other: Any) -> bool:
        return other is self

    def __ne__(self, other: Any) -> bool:
        return other is not self

    def __hash__(self) -> int:
        return 0x5E11  # arbitrary constant

    def __repr__(self) -> str:
        return "NULL"


NULL = _Null()

VALID_TYPES = ("text", "bool", "int")


def is_bool(v: Any) -> bool:
    return isinstance(v, bool)


def check_value(value: Any, declared: str) -> None:
    """Raise ``TypeError`` if *value* does not match declared field type.

    NULL is legal for every nullable field.  Booleans are never accepted as
    ints and vice versa at the type boundaries.
    """
    if value is NULL:
        return
    if declared == "text":
        if not isinstance(value, str):
            raise TypeError(f"expected text, got {type(value).__name__}")
    elif declared == "bool":
        if not is_bool(value):
            raise TypeError(f"expected bool, got {type(value).__name__}")
    elif declared == "int":
        if is_bool(value) or not isinstance(value, int):
            raise TypeError(f"expected int, got {type(value).__name__}")
    else:  # pragma: no cover - declaration is validated earlier
        raise TypeError(f"unknown declared type {declared!r}")


# ---------------------------------------------------------------------------
# Typed JSON codec used for spill files.
#
# Each scalar is encoded as {"#": tag, "v": <json-native value>}.  Tags:
#   n -> NULL sentinel (a real null field value),
#   z -> plain Python None used as an internal record placeholder,
#   s -> text, b -> bool, i -> int (Python's json has no integer size
#   limit on either dump or load, so arbitrary-precision ints round-trip).
# ---------------------------------------------------------------------------


def encode_value(v: Any) -> Any:
    if v is NULL:
        return {"#": "n"}
    if v is None:
        return {"#": "z"}
    if isinstance(v, bool):
        return {"#": "b", "v": v}
    if isinstance(v, str):
        return {"#": "s", "v": v}
    if isinstance(v, int):
        return {"#": "i", "v": v}
    raise TypeError(f"unencodable value of type {type(v).__name__}")


def decode_value(obj: Any) -> Any:
    if isinstance(obj, dict) and "#" in obj:
        tag = obj["#"]
        if tag == "n":
            return NULL
        if tag == "z":
            return None
        if tag == "s":
            v = obj["v"]
            if not isinstance(v, str):
                raise ValueError("corrupt spill record: bad text value")
            return v
        if tag == "b":
            v = obj["v"]
            if not isinstance(v, bool):
                raise ValueError("corrupt spill record: bad bool value")
            return v
        if tag == "i":
            v = obj["v"]
            if isinstance(v, bool) or not isinstance(v, int):
                raise ValueError("corrupt spill record: bad int value")
            return v
        raise ValueError(f"corrupt spill record: unknown tag {tag!r}")
    raise ValueError("corrupt spill record: expected tagged value")


def encode_record(record: Tuple[Any, ...]) -> Any:
    return [encode_value(x) for x in record]


def decode_record(raw: Any) -> Tuple[Any, ...]:
    if not isinstance(raw, list):
        raise ValueError("corrupt spill record: record must be a list")
    return tuple(decode_value(x) for x in raw)
