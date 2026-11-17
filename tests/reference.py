"""Independent in-memory reference implementation for small inputs.

This deliberately uses a completely different algorithm from the engine
(plain dict-of-state plus Python sets), so agreement on many random inputs
is genuine cross-checking rather than two copies of the same logic.  It is
only ever run on tiny inputs in tests.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Sequence, Tuple

from spillagg.declaration import check_declaration
from spillagg.engine import ALL_MARKER, RowError, _convert_row
from spillagg.filter import eval_filter
from spillagg.types import NULL  # noqa: F401  (kept for parity/debugging)


def _load_rows(
    input_paths: Sequence[str], fields
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for path in input_paths:
        with open(path, "r", encoding="utf-8") as fh:
            for lineno, line in enumerate(fh, start=1):
                if not line.strip():
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise RowError(f"{path}:{lineno}: {exc}") from exc
                rows.append(_convert_row(obj, fields))
    return rows


def reference_run(
    input_paths: Sequence[str], declaration: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Return cells in the same shape as the engine's result (minus stats)."""
    normalized = check_declaration(declaration)
    fields = normalized["fields"]
    rows = _load_rows(input_paths, fields)
    aggs = normalized["aggregates"]

    # cells: key (gs_idx, gk) -> list of per-aggregate state dicts
    cells: Dict[Tuple[int, Tuple[Any, ...]], List[Dict[str, Any]]] = {}

    def ensure_cell(gs_idx, gk):
        key = (gs_idx, gk)
        if key not in cells:
            cells[key] = [{} for _ in aggs]
        return cells[key]

    if not rows:
        for gs_idx, gs in enumerate(normalized["grouping_sets"]):
            if len(gs) == 0:
                ensure_cell(gs_idx, ())
    else:
        for row in rows:
            for gs_idx, gs in enumerate(normalized["grouping_sets"]):
                gk = tuple(row[f] for f in gs)
                states = ensure_cell(gs_idx, gk)
                for ai, agg in enumerate(aggs):
                    flt = agg["filter"]
                    if flt is not None and eval_filter(flt, row) is not True:
                        continue
                    op = agg["op"]
                    value = (
                        None if agg["field"] is None else row[agg["field"]]
                    )
                    if op == "count":
                        states[ai]["count_star"] = (
                            states[ai].get("count_star", 0) + 1
                        )
                    elif value is NULL:
                        continue
                    elif op == "count_value":
                        states[ai]["count_value"] = (
                            states[ai].get("count_value", 0) + 1
                        )
                    elif op == "sum":
                        states[ai]["sum_seen"] = (
                            states[ai].get("sum_seen", 0) + 1
                        )
                        states[ai]["sum"] = states[ai].get("sum", 0) + value
                    elif op == "min":
                        cur = states[ai].get("min")
                        states[ai]["min"] = value if cur is None else min(cur, value)
                    elif op == "max":
                        cur = states[ai].get("max")
                        states[ai]["max"] = value if cur is None else max(cur, value)
                    elif op in ("count_distinct", "sum_distinct"):
                        states[ai].setdefault("distinct", set()).add(value)

    out: List[Dict[str, Any]] = []
    for (gs_idx, gk), states in sorted(
        cells.items(), key=lambda kv: (kv[0][0], kv[0][1])
    ):
        gs = set(normalized["grouping_sets"][gs_idx])
        gid = 0
        for i, (name, _) in enumerate(fields):
            if name not in gs:
                gid |= 1 << i
        vals = dict(zip(normalized["grouping_sets"][gs_idx], gk))
        coords = {}
        for name, _ftype in fields:
            if name in vals:
                coords[name] = None if vals[name] is NULL else vals[name]
            else:
                coords[name] = ALL_MARKER
        agg_values = {}
        for ai, agg in enumerate(aggs):
            st = states[ai]
            op = agg["op"]
            if op in ("count", "count_value", "count_distinct"):
                if op == "count_distinct":
                    val = len(st.get("distinct", ()))
                else:
                    key = "count_star" if op == "count" else "count_value"
                    val = st.get(key, 0)
            elif op == "sum":
                val = st.get("sum") if st.get("sum_seen", 0) else None
            elif op == "sum_distinct":
                d = st.get("distinct")
                val = sum(d) if d else None
            elif op in ("min", "max"):
                val = st.get(op)
            else:  # pragma: no cover
                raise AssertionError(op)
            agg_values[agg["name"]] = val
        out.append(
            {
                "grouping_set": gs_idx,
                "grouping_id": gid,
                "coords": coords,
                "aggregates": agg_values,
            }
        )
    return out
