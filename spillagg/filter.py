"""FILTER expressions: declaration validation and three-valued evaluation.

AST (JSON-serialisable)::

    {"op": "and", "args": [expr, ...]}
    {"op": "or",  "args": [expr, ...]}
    {"op": "not", "arg": expr}
    {"op": "eq", "left": operand, "right": operand}
    {"op": "lt", "left": operand, "right": operand}
    {"op": "is_null", "arg": operand}

operand:
    {"field": "name"}
    {"literal": null | true | false | <int> | "<text>"}

Ordinary ``eq``/``lt`` comparisons use SQL three-valued logic: a NULL on
either side yields UNKNOWN (Python ``None``); only rows evaluating to TRUE
contribute.  ``is_null`` always yields a definite boolean.  Booleans are not
orderable and not comparable to ints/text; such declarations are rejected up
front.  Text comparison is Python string comparison, i.e. Unicode code point
order with no normalisation.
"""

from __future__ import annotations

from typing import Any, Dict

from .types import NULL, is_bool


class DeclarationError(ValueError):
    """Raised during check() for any malformed aggregation declaration."""


# ---------------------------------------------------------------------------
# Validation (runs once per declaration, before any row is read)
# ---------------------------------------------------------------------------


def _validate_operand(operand: Any, field_types: Dict[str, str]) -> str:
    """Return the static type of an operand; raise DeclarationError."""
    if not isinstance(operand, dict) or len(operand) != 1:
        raise DeclarationError(
            "filter operand must be {'field': ...} or {'literal': ...}"
        )
    if "field" in operand:
        name = operand["field"]
        if not isinstance(name, str) or name not in field_types:
            raise DeclarationError(f"filter references unknown field {name!r}")
        return field_types[name]
    if "literal" in operand:
        lit = operand["literal"]
        if lit is None:
            return "null"
        if isinstance(lit, bool):
            return "bool"
        if isinstance(lit, str):
            return "text"
        if isinstance(lit, int):
            return "int"
        raise DeclarationError(
            f"filter literal {lit!r} must be null, bool, int or text"
        )
    raise DeclarationError(
        "filter operand must be {'field': ...} or {'literal': ...}"
    )


def _comparable(t1: str, t2: str, op: str) -> str:
    """Check two static operand types for a comparison; return result type."""
    pair = {t1, t2}
    if "null" in pair:
        # Comparison against a NULL literal is legal but always UNKNOWN.
        other = (pair - {"null"}).pop() if pair != {"null"} else "null"
        if other in ("null", "text", "int", "bool"):
            return "tri"
        raise DeclarationError("filter literal has unsupported type")
    if t1 != t2:
        raise DeclarationError(
            f"filter {op}: type mismatch {t1!r} vs {t2!r}"
        )
    if op == "lt" and t1 == "bool":
        raise DeclarationError("filter lt is not defined on bool")
    if t1 not in ("text", "int", "bool"):
        raise DeclarationError(f"filter {op}: unsupported type {t1!r}")
    return "tri"


def validate_filter(expr: Any, field_types: Dict[str, str]) -> None:
    if not isinstance(expr, dict) or "op" not in expr:
        raise DeclarationError("filter expression must be an object with op")
    op = expr["op"]
    if op in ("and", "or"):
        args = expr.get("args")
        if not isinstance(args, list) or not args:
            raise DeclarationError(f"filter {op} requires a non-empty args list")
        for sub in args:
            validate_filter(sub, field_types)
    elif op == "not":
        if "arg" not in expr:
            raise DeclarationError("filter not requires arg")
        validate_filter(expr["arg"], field_types)
    elif op in ("eq", "lt"):
        if "left" not in expr or "right" not in expr:
            raise DeclarationError(f"filter {op} requires left and right")
        t1 = _validate_operand(expr["left"], field_types)
        t2 = _validate_operand(expr["right"], field_types)
        _comparable(t1, t2, op)
    elif op == "is_null":
        if "arg" not in expr:
            raise DeclarationError("filter is_null requires arg")
        _validate_operand(expr["arg"], field_types)
    else:
        raise DeclarationError(f"unknown filter op {op!r}")


# ---------------------------------------------------------------------------
# Three-valued evaluation.  Returns True / False / None (UNKNOWN).
# ---------------------------------------------------------------------------


def _eval_operand(operand: Dict[str, Any], row: Dict[str, Any]) -> Any:
    if "field" in operand:
        return row[operand["field"]]
    lit = operand["literal"]
    if lit is None:
        return NULL
    return lit


def _kleene_and(values):
    saw_null = False
    for v in values:
        if v is False:
            return False
        if v is None:
            saw_null = True
    return None if saw_null else True


def _kleene_or(values):
    saw_null = False
    for v in values:
        if v is True:
            return True
        if v is None:
            saw_null = True
    return None if saw_null else False


def eval_filter(expr: Dict[str, Any], row: Dict[str, Any]) -> Any:
    op = expr["op"]
    if op == "and":
        return _kleene_and(eval_filter(s, row) for s in expr["args"])
    if op == "or":
        return _kleene_or(eval_filter(s, row) for s in expr["args"])
    if op == "not":
        v = eval_filter(expr["arg"], row)
        return None if v is None else (not v)
    if op == "is_null":
        return _eval_operand(expr["arg"], row) is NULL
    # eq / lt
    left = _eval_operand(expr["left"], row)
    right = _eval_operand(expr["right"], row)
    if left is NULL or right is NULL:
        return None
    if op == "eq":
        # bool vs int can never reach here due to static validation; this
        # guard also keeps 1/True distinct if declarations are bypassed.
        if is_bool(left) != is_bool(right):
            return None
        return left == right
    return left < right
