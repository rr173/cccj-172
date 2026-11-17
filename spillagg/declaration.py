"""Aggregation-declaration validation and normalisation."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Tuple

from .filter import DeclarationError, validate_filter

AGG_OPS = (
    "count",            # count(*)
    "count_value",      # count(value)
    "sum",
    "min",
    "max",
    "count_distinct",
    "sum_distinct",
)

_INT_ONLY = {"sum", "sum_distinct"}
_MIN_MAX_TYPES = {"int", "text"}
_DISTINCT_OPS = {"count_distinct", "sum_distinct"}


def _stable_canonical(decl: Any) -> bytes:
    return json.dumps(
        decl, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def check_declaration(decl: Any) -> Dict[str, Any]:
    """Validate *decl* and return a normalised, immutable-ish description.

    Raises :class:`DeclarationError` for every structural/semantic problem,
    including duplicate grouping sets.  No input row may be read before this
    succeeds.
    """
    if not isinstance(decl, dict):
        raise DeclarationError("declaration must be an object")

    raw_fields = decl.get("fields")
    if not isinstance(raw_fields, list) or not raw_fields:
        raise DeclarationError("fields must be a non-empty list")
    field_types: Dict[str, str] = {}
    fields: List[Tuple[str, str]] = []
    for spec in raw_fields:
        if not isinstance(spec, dict) or set(spec) != {"name", "type"}:
            raise DeclarationError("each field needs exactly name and type")
        name, ftype = spec["name"], spec["type"]
        if not isinstance(name, str) or not name:
            raise DeclarationError("field name must be a non-empty string")
        if name in field_types:
            raise DeclarationError(f"duplicate field name {name!r}")
        if ftype not in ("text", "bool", "int"):
            raise DeclarationError(
                f"field {name!r} has unsupported type {ftype!r}"
            )
        field_types[name] = ftype
        fields.append((name, ftype))

    raw_sets = decl.get("grouping_sets")
    if not isinstance(raw_sets, list) or not raw_sets:
        raise DeclarationError("grouping_sets must be a non-empty list")
    grouping_sets: List[Tuple[str, ...]] = []
    seen_sets = set()
    for gs in raw_sets:
        if not isinstance(gs, list):
            raise DeclarationError("each grouping set must be a list of fields")
        gs_tuple = tuple(gs)
        if any(not isinstance(f, str) for f in gs_tuple):
            raise DeclarationError("grouping set entries must be field names")
        if len(set(gs_tuple)) != len(gs_tuple):
            raise DeclarationError(f"duplicate field inside grouping set {gs!r}")
        for f in gs_tuple:
            if f not in field_types:
                raise DeclarationError(
                    f"grouping set references unknown field {f!r}"
                )
        key = frozenset(gs_tuple)
        if key in seen_sets:
            raise DeclarationError(
                f"duplicate grouping set {sorted(gs_tuple)!r} is not allowed"
            )
        seen_sets.add(key)
        grouping_sets.append(gs_tuple)

    raw_aggs = decl.get("aggregates")
    if not isinstance(raw_aggs, list) or not raw_aggs:
        raise DeclarationError("aggregates must be a non-empty list")
    aggregates: List[Dict[str, Any]] = []
    seen_names = set()
    for spec in raw_aggs:
        if not isinstance(spec, dict):
            raise DeclarationError("each aggregate must be an object")
        name = spec.get("name")
        op = spec.get("op")
        if not isinstance(name, str) or not name:
            raise DeclarationError("aggregate name must be a non-empty string")
        if name in seen_names:
            raise DeclarationError(f"duplicate aggregate name {name!r}")
        seen_names.add(name)
        if op not in AGG_OPS:
            raise DeclarationError(f"aggregate {name!r}: unsupported op {op!r}")

        field = spec.get("field")
        if op == "count":
            if field is not None:
                raise DeclarationError(
                    f"aggregate {name!r}: count(*) must not name a field"
                )
        else:
            if not isinstance(field, str) or field not in field_types:
                raise DeclarationError(
                    f"aggregate {name!r}: field {field!r} is unknown"
                )
        ftype = field_types[field] if isinstance(field, str) else None
        if op in _INT_ONLY and ftype != "int":
            raise DeclarationError(
                f"aggregate {name!r}: {op} requires an int field"
            )
        if op in ("min", "max") and ftype not in _MIN_MAX_TYPES:
            raise DeclarationError(
                f"aggregate {name!r}: {op} requires int or text"
            )

        flt = spec.get("filter")
        if flt is not None:
            validate_filter(flt, field_types)

        aggregates.append(
            {
                "name": name,
                "op": op,
                "field": field,
                "field_type": ftype,
                "filter": flt,
                "distinct": op in _DISTINCT_OPS,
            }
        )

    digest = hashlib.sha256(_stable_canonical(decl)).hexdigest()

    return {
        "fields": fields,
        "field_types": field_types,
        "grouping_sets": grouping_sets,
        "aggregates": aggregates,
        "digest": digest,
    }
